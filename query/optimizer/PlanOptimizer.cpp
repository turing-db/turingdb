#include "PlanOptimizer.h"

#include "LocalMemory.h"
#include "PlanGraph.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/FilterNode.h"
#include "nodes/ScanNodesByLabelNode.h"
#include "nodes/ConstScanNode.h"
#include "nodes/VarNode.h"

#include "expr/BinaryExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"
#include "Predicate.h"

#include "columns/ColumnIDs.h"

using namespace db;

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
static bool collectNodeIDsFromOrChain(const Expr* expr,
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
