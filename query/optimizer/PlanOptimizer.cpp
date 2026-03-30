#include "PlanOptimizer.h"

#include "LocalMemory.h"
#include <string_view>

#include "LocalMemory.h"

#include "ID.h"
#include "PlanGraph.h"
#include "CypherAST.h"
#include "columns/ColumnVector.h"
#include "expr/Expr.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
#include "metadata/PropertyType.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/IndexLookupNode.h"
#include "nodes/PlanGraphNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/FilterNode.h"
#include "nodes/ScanNodesByLabelNode.h"
#include "nodes/ConstScanNode.h"
#include "nodes/ConstWriteSourceNode.h"
#include "nodes/CartesianProductNode.h"
#include "nodes/WriteNode.h"
#include "nodes/VarNode.h"

#include "indexes/Index.h"
#include "ExprUtils.h"
#include "expr/BinaryExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"
#include "Predicate.h"
#include "Symbol.h"
#include "decl/VarDecl.h"
#include "decl/DeclContext.h"

#include "columns/ColumnIDs.h"

#include "BioAssert.h"
#include "columns/Column.h"
#include "columns/ColumnVector.h"

using namespace db;

IndexLookupNode* PlanOptimizer::addIndexLookup(const PropertyExpr* propExpr,
                                               const LiteralExpr* litExpr) {
    const Literal* lit = litExpr->getLiteral();
    bioassert(lit, "Null literal.");

    const std::string_view propName = propExpr->getPropName();
    const GraphMetadata& md = _view.metadata();
    const PropertyTypeMap& props = md.propTypes();
    const std::optional<PropertyType> maybeProp = props.get(propName);
    bioassert(maybeProp.has_value(), "Invalid property to index.");
    const PropertyType prop = *maybeProp;
    const PropertyTypeID propID = prop._id;

    const Index* matchingIndex = nullptr;
    // TODO: Check if there is more than one index
    for (const WeakArc<Index>& index : _view.indexes()) {
        const PropertyTypeID indexedProp = index->property();
        if (propID == indexedProp) {
            matchingIndex = index.get();
            break;
        }
    }

    if (!matchingIndex) {
        return nullptr;
    }

    switch (litExpr->getType()) {
        case EvaluatedType::Integer: {
            const ValueType vt = ValueType::Int64;
            const auto* intLit = static_cast<const IntegerLiteral*>(litExpr->getLiteral());

            const int64_t intVal = intLit->getValue();
            auto* queryCol = _mem->alloc<ColumnVector<types::Int64::Primitive>>(1, intVal);

            auto* queryNode = _plan->create<ConstScanNode>(queryCol);

            auto* node = _plan->newOut<IndexLookupNode>(queryNode, matchingIndex,
                                                        propExpr, vt, litExpr);
            return node;
        }
        break;

        case EvaluatedType::Double: {
            const ValueType vt = ValueType::Double;
            const auto* dblLit =
                static_cast<const DoubleLiteral*>(litExpr->getLiteral());

            const double dblVal = dblLit->getValue();
            auto* queryCol =
                _mem->alloc<ColumnVector<types::Double::Primitive>>(1, dblVal);

            auto* queryNode = _plan->create<ConstScanNode>(queryCol);

            auto* node = _plan->newOut<IndexLookupNode>(queryNode, matchingIndex,
                                                        propExpr, vt, litExpr);
            return node;
        }
        break;

        case EvaluatedType::String: {
            const ValueType vt = ValueType::String;
            const auto* strLit = static_cast<const StringLiteral*>(litExpr->getLiteral());

            const std::string_view strVal = strLit->getValue();
            auto* queryCol = _mem->alloc<ColumnVector<types::String::Primitive>>(1, strVal);

            auto* queryNode = _plan->create<ConstScanNode>(queryCol);

            auto* node = _plan->newOut<IndexLookupNode>(queryNode, matchingIndex,
                                                        propExpr, vt, litExpr);
            return node;
        }
        break;

        case EvaluatedType::Bool: {
            const ValueType vt = ValueType::Bool;
            const auto* boolLit = static_cast<const BoolLiteral*>(litExpr->getLiteral());

            const bool boolVal = boolLit->getValue();
            auto* queryCol =
                _mem->alloc<ColumnVector<types::Bool::Primitive>>(1, boolVal);

            auto* queryNode = _plan->create<ConstScanNode>(queryCol);

            auto* node =
                _plan->newOut<IndexLookupNode>(queryNode, matchingIndex, propExpr, vt, litExpr);
            return node;
        }
        break;

        case EvaluatedType::Embedding: {
            const ValueType vt = ValueType::Embedding;
            const auto* embLit = static_cast<const EmbeddingLiteral*>(litExpr->getLiteral());

            const std::span<const float> embVal = embLit->getValue();
            auto* queryCol = _mem->alloc<ColumnVector<types::Embedding::Primitive>>(1, embVal);

            auto* queryNode = _plan->create<ConstScanNode>(queryCol);

            auto* node = _plan->newOut<IndexLookupNode>(queryNode, matchingIndex,
                                                        propExpr, vt, litExpr);
            return node;
        }
        break;

        case EvaluatedType::Null:
        case EvaluatedType::Char:
        case EvaluatedType::List:
        case EvaluatedType::Map:
        case EvaluatedType::Wildcard:
        case EvaluatedType::Tuple:
        case EvaluatedType::ValueType:
        case EvaluatedType::StringTable:
        case EvaluatedType::Invalid:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
        case EvaluatedType::GraphPath:
        case EvaluatedType::_SIZE:
        case EvaluatedType::Label:
        case EvaluatedType::LabelSet:
        case EvaluatedType::PropertyType:
        case EvaluatedType::EdgeType:
            break;
    }

    return nullptr;
}

PlanOptimizer::PlanOptimizer(PlanGraph* plan, GraphView view, LocalMemory* mem)
    : _plan(plan),
    _view(view),
    _mem(mem)
{
}

PlanOptimizer::~PlanOptimizer() {
}

void PlanOptimizer::optimize() {
    // Do some very simple plan rewriting

    rewriteScanByLabels();
    rewriteScanByConstIDs();
    rewriteNodePropertyFilterWithIndex();

    _plan->removeIsolatedNodes();
}

void PlanOptimizer::rewriteScanByLabels() {
    std::vector<PlanGraphNode*> roots;
    _plan->getRoots(roots);

    for (PlanGraphNode* root : roots) {
        // === Check rewrite rule precondition === 
        // We are looking for pairs:
        // [root] ScanNodesNode --> NodeFilterNode (label, no predicates)
        //
        // We don't rewrite if ScanNodesNode has multiple successors
        ScanNodesNode* scanNodes = dynamic_cast<ScanNodesNode*>(root);
        if (!scanNodes) {
            continue;
        } 

        const auto& scanNodesOutputs = scanNodes->outputs();
        if (scanNodesOutputs.size() != 1) {
            continue;
        }

        NodeFilterNode* filterNode = dynamic_cast<NodeFilterNode*>(scanNodesOutputs[0]);
        if (!filterNode) {
            continue;
        }

        // The filter node must have a label constraint and no predicates
        const LabelSet& labelset = filterNode->getLabelConstraints();
        if (labelset.empty() || !filterNode->getPredicates().empty()) {
            continue;
        }
        
        // === Rewrite ===
        
        // Create ScanNodesByLabel
        ScanNodesByLabelNode* scanNodesByLabel = _plan->create<ScanNodesByLabelNode>(labelset);

        // Connect ScanNodesByLabel to the successors of the filter node
        for (PlanGraphNode* filterNodeNext : filterNode->outputs()) {
            scanNodesByLabel->connectOut(filterNodeNext);
        }
        
        scanNodes->clearOutputs();
        filterNode->clearOutputs();
    }
}

// Returns true if expr is a chain of OR'd equalities (var = integer_literal),
// and appends the extracted NodeIDs to nodeIDs.
bool collectNodeIDsFromOrChain(const Expr* expr,
                               const VarDecl* varDecl,
                               ColumnNodeIDs& nodeIDs) {
    if (expr->getKind() != Expr::Kind::BINARY) {
        return false;
    }

    const BinaryExpr* binExpr = static_cast<const BinaryExpr*>(expr);
    const BinaryOperator op = binExpr->getOperator();

    if (op == BinaryOperator::Or) {
        return collectNodeIDsFromOrChain(binExpr->getLHS(), varDecl, nodeIDs)
            && collectNodeIDsFromOrChain(binExpr->getRHS(), varDecl, nodeIDs);
    }

    if (op != BinaryOperator::Equal) {
        return false;
    }

    // Match: symbol = integer_literal  (either order)
    const Expr* lhs = binExpr->getLHS();
    const Expr* rhs = binExpr->getRHS();
    const Expr::Kind lhsKind = lhs->getKind();
    const Expr::Kind rhsKind = rhs->getKind();

    const SymbolExpr* symbolExpr = nullptr;
    const LiteralExpr* literalExpr = nullptr;

    if (lhsKind == Expr::Kind::SYMBOL && rhsKind == Expr::Kind::LITERAL) {
        symbolExpr = static_cast<const SymbolExpr*>(lhs);
        literalExpr = static_cast<const LiteralExpr*>(rhs);
    } else if (lhsKind == Expr::Kind::LITERAL && rhsKind == Expr::Kind::SYMBOL) {
        symbolExpr = static_cast<const SymbolExpr*>(rhs);
        literalExpr = static_cast<const LiteralExpr*>(lhs);
    } else {
        return false;
    }

    if (symbolExpr->getDecl() != varDecl) {
        return false;
    }

    const Literal* literal = literalExpr->getLiteral();
    if (literal->getKind() != Literal::Kind::INTEGER) {
        return false;
    }

    const IntegerLiteral* intLiteral = static_cast<const IntegerLiteral*>(literal);
    if (intLiteral->getValue() < 0) {
        return false;
    }

    nodeIDs.emplace_back(static_cast<uint64_t>(intLiteral->getValue()));
    return true;
}

// Recursively walks the input tree of a WriteNode, accepting only
// VarNode, ConstScanNode, and CartesianProductNode. Collects
// (VarDecl, ConstScanNode) pairs from VarNode→ConstScan edges,
// and appends every visited node to oldNodes for later disconnection.
bool collectConstWriteInputs(PlanGraphNode* node,
                             std::vector<std::pair<const VarDecl*, ConstScanNode*>>& pairs,
                             std::vector<PlanGraphNode*>& oldNodes) {
    if (auto* varNode = dynamic_cast<VarNode*>(node)) {
        const auto& inputs = varNode->inputs();
        if (inputs.size() != 1) {
            return false;
        }

        auto* constScan = dynamic_cast<ConstScanNode*>(inputs[0]);
        if (!constScan) {
            return false;
        }

        pairs.emplace_back(varNode->getVarDecl(), constScan);
        oldNodes.push_back(varNode);
        oldNodes.push_back(constScan);
        return true;
    } else if (dynamic_cast<CartesianProductNode*>(node)) {
        oldNodes.push_back(node);
        for (PlanGraphNode* input : node->inputs()) {
            if (!collectConstWriteInputs(input, pairs, oldNodes)) {
                return false;
            }
        }
        return true;
    }

    return false;
}

template <typename ColType, typename LiteralType>
Column* buildTypedValuesColumn(LocalMemory* mem,
                               const WriteNode::NodeUpdateSpan& updates,
                               const std::unordered_map<const VarDecl*, ConstScanNode*>& varToScan) {
    auto* col = mem->alloc<ColumnVector<ColType>>();
    for (const auto& [decl, propName, expr] : updates) {
        const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(expr);
        const LiteralType* lit = static_cast<const LiteralType*>(litExpr->getLiteral());
        const size_t count = varToScan.at(decl)->values()->size();
        for (size_t i = 0; i < count; ++i) {
            col->emplace_back(lit->getValue());
        }
    }
    return col;
}

// Build a typed values column by repeating each literal for every NodeID
// in the corresponding ConstScan. Dispatches on the first literal's kind.
Column* buildValuesColumn(LocalMemory* mem,
                          const WriteNode::NodeUpdateSpan& updates,
                          const std::unordered_map<const VarDecl*, ConstScanNode*>& varToScan) {

    const auto* firstLitExpr = static_cast<const LiteralExpr*>(updates[0]._propValueExpr);
    const Literal::Kind kind = firstLitExpr->getLiteral()->getKind();

    switch (kind) {
        case Literal::Kind::INTEGER:
            return buildTypedValuesColumn<int64_t, IntegerLiteral>(mem, updates, varToScan);
        break;

        case Literal::Kind::DOUBLE:
            return buildTypedValuesColumn<double, DoubleLiteral>(mem, updates, varToScan);
        break;

        case Literal::Kind::STRING:
            return buildTypedValuesColumn<std::string_view, StringLiteral>(mem, updates, varToScan);
        break;

        case Literal::Kind::BOOL:
            return buildTypedValuesColumn<CustomBool, BoolLiteral>(mem, updates, varToScan);
        break;

        case Literal::Kind::EMBEDDING:
            return buildTypedValuesColumn<std::span<const float>, EmbeddingLiteral>(mem, updates, varToScan);
        break;

        default:
            return nullptr;
        break;
    }
}

EvaluatedType literalKindToEvaluatedType(Literal::Kind kind) {
    switch (kind) {
        case Literal::Kind::INTEGER:
            return EvaluatedType::Integer;
        break;

        case Literal::Kind::DOUBLE:
            return EvaluatedType::Double;
        break;

        case Literal::Kind::STRING:
            return EvaluatedType::String;
        break;

        case Literal::Kind::BOOL:
            return EvaluatedType::Bool;
        break;

        case Literal::Kind::EMBEDDING:
            return EvaluatedType::Embedding;
        break;

        default:
            return EvaluatedType::Invalid;
        break;
    }
}

}

PlanOptimizer::PlanOptimizer(PlanGraph* plan, GraphView view, LocalMemory* mem, CypherAST* ast)
    : _plan(plan),
    _view(view),
    _mem(mem),
    _ast(ast)
{
}

PlanOptimizer::~PlanOptimizer() {
}

void PlanOptimizer::optimize() {
    // Do some very simple plan rewriting

    rewriteScanByLabels();
    rewriteScanByConstIDs();
    rewriteConstWriteSources();

    _plan->removeIsolatedNodes();
}

void PlanOptimizer::rewriteScanByLabels() {
    std::vector<PlanGraphNode*> roots;
    _plan->getRoots(roots);

    for (PlanGraphNode* root : roots) {
        // === Check rewrite rule precondition === 
        // We are looking for pairs:
        // [root] ScanNodesNode --> NodeFilterNode (label, no predicates)
        //
        // We don't rewrite if ScanNodesNode has multiple successors
        ScanNodesNode* scanNodes = dynamic_cast<ScanNodesNode*>(root);
        if (!scanNodes) {
            continue;
        } 

        const auto& scanNodesOutputs = scanNodes->outputs();
        if (scanNodesOutputs.size() != 1) {
            continue;
        }

        NodeFilterNode* filterNode = dynamic_cast<NodeFilterNode*>(scanNodesOutputs[0]);
        if (!filterNode) {
            continue;
        }

        // The filter node must have a label constraint and no predicates
        const LabelSet& labelset = filterNode->getLabelConstraints();
        if (labelset.empty() || !filterNode->getPredicates().empty()) {
            continue;
        }
        
        // === Rewrite ===
        
        // Create ScanNodesByLabel
        ScanNodesByLabelNode* scanNodesByLabel = _plan->create<ScanNodesByLabelNode>(labelset);

        // Connect ScanNodesByLabel to the successors of the filter node
        for (PlanGraphNode* filterNodeNext : filterNode->outputs()) {
            scanNodesByLabel->connectOut(filterNodeNext);
        }
        
        scanNodes->clearOutputs();
        filterNode->clearOutputs();
    }
}

void PlanOptimizer::rewriteScanByConstIDs() {
    std::vector<PlanGraphNode*> roots;
    _plan->getRoots(roots);

    for (PlanGraphNode* root : roots) {
        // === Check rewrite rule precondition ===
        // We are looking for pairs:
        // [root] ScanNodesNode --> NodeFilterNode (no labels, single predicate)
        // where the predicate is a chain of OR'd equalities on NodeIDs:
        //   n = 42  OR  n = 42 OR n = 53  etc.

        ScanNodesNode* scanNodes = dynamic_cast<ScanNodesNode*>(root);
        if (!scanNodes) {
            continue;
        }

        const auto& scanNodesOutputs = scanNodes->outputs();
        if (scanNodesOutputs.size() != 1) {
            continue;
        }

        NodeFilterNode* filterNode = dynamic_cast<NodeFilterNode*>(scanNodesOutputs[0]);
        if (!filterNode) {
            continue;
        }

        if (!filterNode->getLabelConstraints().empty()) {
            continue;
        }

        const auto& predicates = filterNode->getPredicates();
        if (predicates.size() != 1) {
            continue;
        }

        const VarDecl* varDecl = filterNode->getVarNode()->getVarDecl();
        const Expr* predExpr = predicates[0]->getExpr();

        ColumnNodeIDs* nodeIDs = _mem->alloc<ColumnNodeIDs>();
        if (!collectNodeIDsFromOrChain(predExpr, varDecl, *nodeIDs)) {
            continue;
        }

        // === Rewrite ===

        ConstScanNode* constScan = _plan->create<ConstScanNode>(nodeIDs);

        for (PlanGraphNode* filterNodeNext : filterNode->outputs()) {
            constScan->connectOut(filterNodeNext);
        }

        scanNodes->clearOutputs();
        filterNode->clearOutputs();
    }
}

void PlanOptimizer::rewriteConstWriteSources() {
    // Iterate over a snapshot of node pointers since we may add new nodes
    std::vector<PlanGraphNode*> snapshot;
    snapshot.reserve(_plan->nodes().size());
    for (const auto& node : _plan->nodes()) {
        snapshot.push_back(node.get());
    }

    std::vector<std::pair<const VarDecl*, ConstScanNode*>> pairs;
    std::vector<PlanGraphNode*> oldNodes;
    std::unordered_map<const VarDecl*, ConstScanNode*> varToScan;
    
    DeclContext* optDeclCtx = DeclContext::create(_ast, nullptr);

    for (PlanGraphNode* node : snapshot) {
        WriteNode* writeNode = dynamic_cast<WriteNode*>(node);
        if (!writeNode) {
            continue;
        }

        // === Precondition checks ===

        if (writeNode->inputs().empty()) {
            continue;
        }

        if (!writeNode->pendingNodes().empty()
            || !writeNode->pendingEdges().empty()
            || !writeNode->toDeleteNodes().empty()
            || !writeNode->toDeleteEdges().empty()
            || !writeNode->edgeUpdates().empty()) {
            continue;
        }

        const auto& updates = writeNode->nodeUpdates();
        if (updates.empty()) {
            continue;
        }

        // All nodeUpdates must target the same property name and use literal values
        const std::string_view propName = updates[0]._propTypeName;
        bool allValid = true;

        for (const auto& update : updates) {
            if (update._propTypeName != propName) {
                allValid = false;
                break;
            }
            if (update._propValueExpr->getKind() != Expr::Kind::LITERAL) {
                allValid = false;
                break;
            }
        }

        if (!allValid) {
            continue;
        }

        // All literals must be the same kind
        const LiteralExpr* firstLiteralExpr = static_cast<const LiteralExpr*>(updates[0]._propValueExpr);
        const auto firstKind = firstLiteralExpr->getLiteral()->getKind();
        for (const auto& update : updates) {
            const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(update._propValueExpr);
            if (litExpr->getLiteral()->getKind() != firstKind) {
                allValid = false;
                break;
            }
        }

        if (!allValid) {
            continue;
        }

        // Walk input tree: collect VarDecl → ConstScanNode mapping
        if (writeNode->inputs().size() != 1) {
            continue;
        }

        pairs.clear();
        oldNodes.clear();
        if (!collectConstWriteInputs(writeNode->inputs()[0], pairs, oldNodes)) {
            continue;
        }

        // Build lookup map and verify every nodeUpdate VarDecl is covered
        for (const auto& [decl, scan] : pairs) {
            varToScan[decl] = scan;
        }

        for (const auto& update : updates) {
            if (!varToScan.contains(update._decl)) {
                allValid = false;
                break;
            }
        }

        if (!allValid) {
            continue;
        }

        // === Build flattened columns ===

        ColumnNodeIDs* flatNodeIDs = _mem->alloc<ColumnNodeIDs>();
        for (const auto& update : updates) {
            const auto* srcCol = dynamic_cast<const ColumnNodeIDs*>(varToScan.at(update._decl)->values());
            bioassert(srcCol, "ConstScan values column is not ColumnNodeIDs");
            for (size_t i = 0; i < srcCol->size(); ++i) {
                flatNodeIDs->emplace_back((*srcCol)[i]);
            }
        }

        Column* flatValues = buildValuesColumn(_mem, updates, varToScan);
        if (!flatValues) {
            continue;
        }

        // === Create artificial VarDecls and Expr via standard AST APIs ===
        std::string* nidName = _ast->createString();
        nidName->assign("__cws_nid");
        VarDecl* nodeIDDecl = VarDecl::create(_ast,
                                              optDeclCtx,
                                              *nidName,
                                              EvaluatedType::NodePattern);

        const EvaluatedType valType = literalKindToEvaluatedType(firstKind);

        std::string* valName = _ast->createString();
        valName->assign("__cws_val");
        VarDecl* valuesDecl = VarDecl::create(_ast, optDeclCtx, *valName, valType);

        Symbol* valSymbol = Symbol::create(_ast, *valName);
        SymbolExpr* valuesExpr = SymbolExpr::create(_ast, valSymbol);
        valuesExpr->setDecl(valuesDecl);
        valuesExpr->setExprVarDecl(valuesDecl);
        valuesExpr->setType(valType);

        // === Rewrite the plan ===
        writeNode->clearInputs();

        for (PlanGraphNode* old : oldNodes) {
            old->clearInputs();
            old->clearOutputs();
        }

        writeNode->clearNodeUpdates();
        writeNode->addNodeUpdate(nodeIDDecl, propName, valuesExpr);

        auto* cwsNode = _plan->create<ConstWriteSourceNode>(flatNodeIDs,
                                                            flatValues,
                                                            nodeIDDecl,
                                                            valuesDecl);
        cwsNode->connectOut(writeNode);
    }
}
/*
 * Looks for
 */
void PlanOptimizer::rewritePropertyFilterWithIndex() {
    rewriteNodePropertyFilterWithIndex();
}

void PlanOptimizer::rewriteNodePropertyFilterWithIndex() {
    // DFS from ScanNodes. Break on VarNode (node has no filters)

    std::vector<PlanGraphNode*> roots;
    _plan->getRoots(roots);
    for (PlanGraphNode* root : roots) {
        auto* scanNodes = dynamic_cast<ScanNodesNode*>(root);
        if (!scanNodes) {
            continue;
        }

        const auto& scanNodesOutput = scanNodes->outputs();
        if(scanNodesOutput.size() != 1) {
            return;
        }

        PlanGraphNode* scanOutput = scanNodesOutput.front();
        auto* getPropsNode = dynamic_cast<GetPropertyWithNullNode*>(scanOutput);
        if (!getPropsNode) {
            continue;
        }

        const auto& gpOutputs = getPropsNode->outputs();
        if (gpOutputs.size() != 1) {
            return;
        }

        PlanGraphNode* gpOutput = gpOutputs.front();
        auto* filterNode = dynamic_cast<NodeFilterNode*>(gpOutput);
        if (!filterNode) {
            continue;
        }

        // TODO: Support label-constrained indexes
        const LabelSet& labelsetConstraint = filterNode->getLabelConstraints();
        if (!labelsetConstraint.empty()) {
            continue;
        }

        const std::vector<Predicate*>& predicates = filterNode->getPredicates();
        // TODO check if all equality in disjunctive normal form, not just single equality
        if (predicates.size() != 1) {
            continue;
        }

        const Predicate* pred = predicates.front();
        const auto* binExpr = dynamic_cast<const BinaryExpr*>(pred->getExpr());
        if (!binExpr) {
            return; // Can occur with Boolean propertyies as predicates e.g. NOT isFrench
        }

        // Index only supports equality
        if (binExpr->getOperator() != BinaryOperator::Equal) {
            continue;
        }

        PropertyExpr* propExpr {nullptr};
        LiteralExpr* literalExpr {nullptr};

        Expr* lhs = binExpr->getLHS();
        Expr* rhs = binExpr->getRHS();

        // Ensure some permutation of {property, literal}
        if (lhs->getKind() == Expr::Kind::LITERAL) {
            literalExpr = static_cast<LiteralExpr*>(lhs);
        } else if (rhs->getKind() == Expr::Kind::LITERAL) {
            literalExpr = static_cast<LiteralExpr*>(rhs);
        }
        if (lhs->getKind() == Expr::Kind::PROPERTY) {
            propExpr = static_cast<PropertyExpr*>(lhs);
        } else if (lhs->getKind() == Expr::Kind::PROPERTY) {
            propExpr = static_cast<PropertyExpr*>(rhs);
        }

        // Not a literal property constraint
        if (!literalExpr || !propExpr)  {
            continue;
        }

        IndexLookupNode* indexNode = addIndexLookup(propExpr, literalExpr);

        if (!indexNode) {
            return;
        }

        { // Rewire plan graph
            const auto& filterSuccessors = filterNode->outputs();
            for (PlanGraphNode* successor : filterSuccessors) {
                indexNode->connectOut(successor);
            }
            scanNodes->clearOutputs();
            filterNode->clearOutputs();
            getPropsNode->clearOutputs();
        }
    }
}

