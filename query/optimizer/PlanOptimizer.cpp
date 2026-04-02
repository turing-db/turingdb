#include "PlanOptimizer.h"

#include <algorithm>
#include <iterator>
#include <string_view>

#include "LocalMemory.h"

#include "ID.h"
#include "PlanGraph.h"
#include "CypherAST.h"
#include "columns/ColumnVector.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
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
#include "decl/DeclContext.h"

#include "columns/ColumnIDs.h"

#include "BioAssert.h"
#include "columns/Column.h"

using namespace db;

PlanOptimizer::PlanOptimizer(PlanGraph* plan,
                             GraphView view,
                             LocalMemory* mem,
                             CypherAST* ast)
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
        if (!ExprUtils::collectFromHomogeneousBinaryChain<ExprUtils::NodeIDEqualsOR>(
                predExpr, varDecl, nodeIDs->getRaw())) {
            continue;
        }

        // === Rewrite ===

        ConstScanNode* constScan = _plan->create<ConstScanNode>(nodeIDs, varDecl);

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

void PlanOptimizer::rewritePropertyFilterWithIndex() {
    rewriteNodePropertyFilterWithIndex();
}

// Finds ScanNodes -> GetProperties -> Filter
// replaces with ConstScan -> IndexLookup
void PlanOptimizer::rewriteNodePropertyFilterWithIndex() {
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
        // Any number of ORs chained together is evaluated as 1 predicate. If we have any
        // more than one predicate, we must have an AND somewhere, which means we cannot
        // use the index
        if (predicates.size() != 1) {
            continue;
        }

        // We can do an index lookup iff we have a chain of
        // x.prop1 = LITERAL OR x.prop1 = LITERAL  OR...
        // We must ensure all property constraints are on the same property, and that all
        // values on the constraints are literals (interpret-time evaluatable)
        const Expr* firstPredicate = predicates.front()->getExpr();
        PropertyExpr* propExpr {nullptr};
        { // Search the expression chain for the leftmost (arbitrary choice) leaf
          // i.e. property expression x.prop = y
            const auto* binExpr = dynamic_cast<const BinaryExpr*>(firstPredicate);
            if (!binExpr) {
                // Can occur with Boolean props as predicates e.g. NOT isFrench, but we
                // cannot use index in such a case
                return;
            }

            // Walk down the left side of any OR chain to find the first leaf equality,
            // from which we extract the property expression used to "anchor" the full
            // chain i.e. assert that all other expressions are on the same property.
            // Since all we need to assertis that all property expressions are on the same
            // property, we can chose any expresion (here the leftmost) as the "anchor".
            const BinaryExpr* leafExpr = binExpr;
            while (leafExpr->getOperator() == BinaryOperator::Or) {
                const auto* lhsBin = dynamic_cast<const BinaryExpr*>(leafExpr->getLHS());
                if (!lhsBin) {
                    break;
                }
                leafExpr = lhsBin;
            }

            if (leafExpr->getOperator() != BinaryOperator::Equal) {
                continue;
            }

            Expr* lhs = leafExpr->getLHS();
            Expr* rhs = leafExpr->getRHS();

            // At least one side of this equality needs to be a property expr
            if (lhs->getKind() == Expr::Kind::PROPERTY) {
                propExpr = static_cast<PropertyExpr*>(lhs);
            } else if (rhs->getKind() == Expr::Kind::PROPERTY) {
                propExpr = static_cast<PropertyExpr*>(rhs);
            }
        }
        // The leaf wasnt a property expr, we cannot use the index
        if (!propExpr) {
            continue;
        }
        // @ref propExpr now contains the property constraint (e.g n.age) which all other
        // expressions need also have

        // Traverse the expression tree, asserting that all property expressions are the
        // same as @ref propExpr, and that all constraint values are literals.
        // Store the literal values into @ref lookupValueLiterals.
        std::vector<const LiteralExpr*> lookupValueLiterals;
        if (!ExprUtils::collectFromHomogeneousBinaryChain<ExprUtils::PropertyEqualsOR>(
                firstPredicate, propExpr, lookupValueLiterals)) {
            continue;
        }

        // Attempt to translate all the literals in @ref lookupValueLiterals to a query
        // for the index lookup. NOTE: Also adds a ConstScan prior to any IndexLookup
        // added.
        IndexLookupNode* indexNode = addIndexLookup(propExpr, lookupValueLiterals);

        if (!indexNode) {
            // Failed to make index query: can't use lookup
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

IndexLookupNode* PlanOptimizer::addIndexLookup(const PropertyExpr* propExpr,
                                               const std::vector<const LiteralExpr*>& litExprs) {
    if (litExprs.empty()) {
        return nullptr;
    }

    const Literal* lit = litExprs.front()->getLiteral();
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

    // Ensure that all the literals are the same type. This should be guaranteed by the
    // analyzer (since they are being assigned to the same property), but check to be sure
    const EvaluatedType type = litExprs.front()->getType();
    {
        const auto sametype = [type](const LiteralExpr* x) {
            return x->getType() == type;
        };

        const bool allSameType = std::ranges::all_of(litExprs, sametype);
        if (!allSameType) {
            return nullptr;
        }
    }

    // Helper to convert literals into a column used to query the index,
    // add a const scan to emit that column as input to the index,
    // and add the index node itself. Returns the index node.
    const auto addIndex = [&]<SupportedType PropertyType, typename LiteralType>(const ValueType valueType) -> IndexLookupNode* {
        using PropertyPrimitive = typename PropertyType::Primitive;
        using PropertyValueColumn = ColumnVector<PropertyPrimitive>;

        // Allocate the query column
        auto* queryCol = _mem->alloc<PropertyValueColumn>();

        // Helper to convert a LiteralExpr* of known derived type (@param LiteralType)
        // to its corresponding value of type @param PropertyPrimitive
        const auto litToVal = [](const LiteralExpr* lit) -> PropertyPrimitive {
            auto* typedLit = dynamic_cast<const LiteralType*>(lit->getLiteral());
            bioassert(typedLit, "Invalid literal."); // TODO: try and remove this
            const PropertyPrimitive value = typedLit->getValue();
            return value;
        };

        std::vector<PropertyPrimitive>& raw = queryCol->getRaw();
        // Generate the column of values from the literals
        std::ranges::transform(litExprs, std::back_inserter(raw), litToVal);

        // Add a const scan node which emits that column of literals
        auto* queryNode = _plan->create<ConstScanNode>(queryCol, propExpr->getExprVarDecl());

        // Wire up an index node, taking hte column of literals as its query to lookup
        auto* indexNode = _plan->newOut<IndexLookupNode>(queryNode, matchingIndex, propExpr, valueType);

        return indexNode;
    };

    switch (type) {
        case EvaluatedType::Integer:
            return addIndex.template operator()<types::Int64, IntegerLiteral>(ValueType::Int64);
        break;

        case EvaluatedType::Double:
            return addIndex.template operator()<types::Double, DoubleLiteral>(ValueType::Double);
        break;

        case EvaluatedType::String:
            return addIndex.template operator()<types::String, StringLiteral>(ValueType::String);
        break;

        case EvaluatedType::Bool:
            return addIndex.template operator()<types::Bool, BoolLiteral>(ValueType::Bool);
        break;

        case EvaluatedType::Embedding:
            return addIndex.template operator()<types::Embedding, EmbeddingLiteral>(ValueType::Embedding);
        break;

        case EvaluatedType::Invalid:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
        case EvaluatedType::GraphPath:
        case EvaluatedType::Null:
        case EvaluatedType::Char:
        case EvaluatedType::List:
        case EvaluatedType::Map:
        case EvaluatedType::Wildcard:
        case EvaluatedType::Tuple:
        case EvaluatedType::ValueType:
        case EvaluatedType::StringTable:
        case EvaluatedType::Label:
        case EvaluatedType::LabelSet:
        case EvaluatedType::PropertyType:
        case EvaluatedType::EdgeType:
        case EvaluatedType::_SIZE:
            return nullptr;
        break;
    }
    return nullptr;
}
