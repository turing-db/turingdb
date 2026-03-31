#include "PlanOptimizer.h"

#include "LocalMemory.h"
#include "PlanGraph.h"
#include "CypherAST.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/FilterNode.h"
#include "nodes/ScanNodesByLabelNode.h"
#include "nodes/ConstScanNode.h"
#include "nodes/ConstWriteSourceNode.h"
#include "nodes/CartesianProductNode.h"
#include "nodes/WriteNode.h"
#include "nodes/VarNode.h"

#include "expr/BinaryExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"
#include "Predicate.h"
#include "Symbol.h"
#include "decl/VarDecl.h"
#include "decl/DeclContext.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"

#include "BioAssert.h"

using namespace db;

namespace {

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
        varToScan.clear();
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
