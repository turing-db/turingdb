#include "WriteStmtGenerator.h"

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "PlanGraph.h"
#include "PlannerException.h"
#include "WhereClause.h"
#include "PlanGraphVariables.h"

#include "decl/VarDecl.h"
#include "decl/PatternData.h"

#include "expr/ExprChain.h"
#include "expr/SymbolExpr.h"

#include "nodes/WriteNode.h"
#include "nodes/VarNode.h"

#include "stmt/Stmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/DeleteStmt.h"

using namespace db::v2;

WriteStmtGenerator::WriteStmtGenerator(const CypherAST* ast,
                                       PlanGraph* tree,
                                       PlanGraphVariables* variables)
    : _ast(ast),
    _tree(tree),
    _variables(variables)
{
}

WriteStmtGenerator::~WriteStmtGenerator() {
}

WriteNode* WriteStmtGenerator::generateStmt(const Stmt* stmt, PlanGraphNode* prevNode) {
    switch (stmt->getKind()) {
        case Stmt::Kind::CREATE:
            generateCreateStmt(static_cast<const CreateStmt*>(stmt), prevNode);
            break;

        case Stmt::Kind::DELETE:
            generateDeleteStmt(static_cast<const DeleteStmt*>(stmt), prevNode);
            break;

        default:
            throwError(fmt::format("Unsupported write statement type: {}", (uint64_t)stmt->getKind()), stmt);
            break;
    }

    return _currentNode;
}

void WriteStmtGenerator::generateCreateStmt(const CreateStmt* stmt, PlanGraphNode* prevNode) {
    _currentNode = prevNode
                     ? _tree->newOut<WriteNode>(prevNode)
                     : _tree->create<WriteNode>();

    const Pattern* pattern = stmt->getPattern();

    for (const PatternElement* element : pattern->elements()) {
        generatePatternElement(element);
    }
}

void WriteStmtGenerator::generateDeleteStmt(const DeleteStmt* stmt, PlanGraphNode* prevNode) {
    _currentNode = prevNode
                     ? _tree->newOut<WriteNode>(prevNode)
                     : _tree->create<WriteNode>();

    const ExprChain* exprs = stmt->getExpressions();

    for (Expr* expr : *exprs) {
        if (expr->getKind() != Expr::Kind::SYMBOL) {
            throwError("Expressions in DELETE statements can only be symbols", expr);
        }

        const SymbolExpr* symbol = static_cast<const SymbolExpr*>(expr);
        const VarDecl* decl = symbol->getDecl();

        if (!decl) [[unlikely]] {
            throwError("Cannot delete entity that does not exist", symbol);
        }

        const EvaluatedType type = symbol->getType();

        if (type == EvaluatedType::NodePattern) {
            _currentNode->deleteNode(decl);
        } else if (type == EvaluatedType::EdgePattern) {
            _currentNode->deleteEdge(decl);
        } else {
            throwError(fmt::format("Can only delete nodes or edges, not '{}'",
                                   EvaluatedTypeName::value(expr->getType())),
                       expr);
        }
    }
}

void WriteStmtGenerator::generatePatternElement(const PatternElement* element) {
    NodePattern* origin = dynamic_cast<NodePattern*>(element->getRootEntity());

    // Step 1. Handle the origin

    if (!origin) [[unlikely]] {
        throwError("CREATE statement must have a node pattern as origin", element);
    }

    const VarDecl* originDecl = origin->getDecl();
    const NodePatternData* data = origin->getData();

    WriteNode::EdgeNeighbour lhs;
    WriteNode::EdgeNeighbour rhs;

    if (_variables->getVarNode(originDecl) != nullptr) {
        lhs = WriteNode::EdgeNeighbour {originDecl};
    } else {
        // Create a new node
        const size_t offset = _currentNode->addNode(data);
        lhs = WriteNode::EdgeNeighbour {offset};
    }

    // Step 2. Handle the [edge-rhs] chains

    for (auto [edge, rhsNode] : element->getElementChain()) {
        // - Get/Create the rhs node
        const VarDecl* rhsDecl = rhsNode->getDecl();
        const NodePatternData* rhsData = rhsNode->getData();

        if (_variables->getVarNode(rhsDecl) != nullptr) {
            rhs = WriteNode::EdgeNeighbour {rhsDecl};
        } else {
            const size_t rhsOffset = _currentNode->addNode(rhsData);
            rhs = WriteNode::EdgeNeighbour {rhsOffset};
        }

        // - Create the new edge
        switch (edge->getDirection()) {
            case EdgePattern::Direction::Undirected:
                throwError("Cannot use undirected edges in write statements", edge);
            case EdgePattern::Direction::Backward:
                _currentNode->addEdge(rhs, edge->getData(), lhs);
                break;
            case EdgePattern::Direction::Forward:
                _currentNode->addEdge(lhs, edge->getData(), rhs);
                break;
        }

        lhs = rhs;
    }
}

void WriteStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    throw PlannerException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
