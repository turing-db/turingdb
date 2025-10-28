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
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"

#include "nodes/WriteNode.h"
#include "nodes/VarNode.h"

#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"
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
    fmt::println("Generating write stmt");
    switch (stmt->getKind()) {
        case Stmt::Kind::CREATE:
            generateCreateStmt(static_cast<const CreateStmt*>(stmt), prevNode);
            break;

        case Stmt::Kind::SET:
            generateSetStmt(static_cast<const SetStmt*>(stmt), prevNode);
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
    prepareWriteNode(prevNode);

    const Pattern* pattern = stmt->getPattern();

    for (const PatternElement* element : pattern->elements()) {
        generateCreatePatternElement(element);
    }
}

void WriteStmtGenerator::generateSetStmt(const SetStmt* stmt, PlanGraphNode* prevNode) {
    prepareWriteNode(prevNode);

    for (const SetItem* item : stmt->getItems()) {
        switch (item->item().index()) {
            case SetItem::PropertyExprAssign::index: {
                const auto& v = std::get<SetItem::PropertyExprAssign>(item->item());
                const VarDecl* decl = v.propTypeExpr->getDecl();
                if (decl->getType() == EvaluatedType::NodePattern) {
                    _currentNode->addNodeUpdate(v.propTypeExpr->getPropName(), v.propValueExpr);
                } else if (decl->getType() == EvaluatedType::EdgePattern) {
                    _currentNode->addEdgeUpdate(v.propTypeExpr->getPropName(), v.propValueExpr);
                }
            } break;
            case SetItem::SymbolAddAssign::index: {
                throwError("SET cannot dynamically mutate properties yet", item);
            } break;
            case SetItem::SymbolEntityTypes::index: {
                throwError("SET cannot update entity types yet", item);
            } break;
            default: {
                throwError(fmt::format("Unsupported SET expression"), item);
            }
        }
    }
}

void WriteStmtGenerator::generateDeleteStmt(const DeleteStmt* stmt, PlanGraphNode* prevNode) {
    prepareWriteNode(prevNode);

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
            if (_currentNode->hasPendingNode(decl)) {
                // TODO: Check if we should support this, Neo4j does
                throwError("Cannot delete pending node", symbol);
            }

            _currentNode->deleteNode(decl);
        } else if (type == EvaluatedType::EdgePattern) {
            if (_currentNode->hasPendingEdge(decl)) {
                // TODO: Check if we should support this, Neo4j does
                throwError("Cannot delete pending edge", symbol);
            }

            _currentNode->deleteEdge(decl);
        } else {
            throwError(fmt::format("Can only delete nodes or edges, not '{}'",
                                   EvaluatedTypeName::value(expr->getType())),
                       expr);
        }
    }
}

void WriteStmtGenerator::generateCreatePatternElement(const PatternElement* element) {
    NodePattern* origin = dynamic_cast<NodePattern*>(element->getRootEntity());

    // Step 1. Handle the origin

    if (!origin) [[unlikely]] {
        throwError("CREATE statement must have a node pattern as origin", element);
    }

    const VarDecl* originDecl = origin->getDecl();
    const NodePatternData* data = origin->getData();

    WriteNode::EdgeNeighbour lhs;
    WriteNode::EdgeNeighbour rhs;

    // A node may:
    //   - Have to be created
    //   - Be an input to the write query: MATCH (n) CREATE (n)-[e:E]->(m)
    //   - Already created in the query: CREATE (n:Person), (n)-[e:E]->(m)
    const VarNode* varNode = _variables->getVarNode(originDecl);

    if (varNode != nullptr || _currentNode->hasPendingNode(originDecl)) {
        // Already defined (input or pending)
        lhs = WriteNode::EdgeNeighbour {originDecl};
    } else {
        // Create a new node
        const size_t offset = _currentNode->addNode(originDecl, data);
        lhs = WriteNode::EdgeNeighbour {offset};
    }

    // Step 2. Handle the [edge-rhs] chains

    for (auto [edge, rhsNode] : element->getElementChain()) {
        // - Get/Create the rhs node
        const VarDecl* rhsDecl = rhsNode->getDecl();
        const NodePatternData* rhsData = rhsNode->getData();
        const VarNode* rhsVarNode = _variables->getVarNode(rhsDecl);

        if (rhsVarNode != nullptr || _currentNode->hasPendingNode(rhsDecl)) {
            // Already defined (input or pending)
            rhs = WriteNode::EdgeNeighbour {rhsDecl};
        } else {
            // Create a new node
            const size_t rhsOffset = _currentNode->addNode(rhsDecl, rhsData);
            rhs = WriteNode::EdgeNeighbour {rhsOffset};
        }

        // - Create the new edge
        const VarDecl* edgeDecl = edge->getDecl();

        switch (edge->getDirection()) {
            case EdgePattern::Direction::Undirected:
                throwError("Cannot use undirected edges in write statements", edge);
            case EdgePattern::Direction::Backward:
                _currentNode->addEdge(edgeDecl, rhs, edge->getData(), lhs);
                break;
            case EdgePattern::Direction::Forward:
                _currentNode->addEdge(edgeDecl, lhs, edge->getData(), rhs);
                break;
        }

        lhs = rhs;
    }
}

void WriteStmtGenerator::prepareWriteNode(PlanGraphNode* prevNode) {
    if (!prevNode) {
        // First node in the plan graph
        _currentNode = _tree->create<WriteNode>();
    } else if (prevNode->getOpcode() == PlanGraphOpcode::WRITE) {
        // Previous node is a write node, reuse it
        _currentNode = static_cast<WriteNode*>(prevNode);
    } else {
        // Previous node is not a write node, create a new one
        _currentNode = _tree->newOut<WriteNode>(prevNode);
    }
}

void WriteStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    throw PlannerException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
