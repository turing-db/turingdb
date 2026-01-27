#include "WriteStmtGenerator.h"

#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "ExprDependencies.h"
#include "Overloaded.h"
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

using namespace db;

namespace {

template <typename T>
    requires (std::is_same_v<const NodePatternData*, T>) || (std::is_same_v<const EdgePatternData*, T>)
void checkDependencies(PlanGraphVariables& variables, T data) {
    ExprDependencies deps;
    for (auto&& d : data->exprConstraints()) {
        deps.genExprDependencies(variables, d._expr);
    }
    if (!deps.empty()) {
        throw PlannerException("CREATE statements with entity dependencies "
                               "are not yet supported.");
    }
}

}

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
        const auto visitor = Overloaded {
            // PropertyExprAssign case
            [this](const SetItem::PropertyExprAssign& v) {
                const VarDecl* decl = v.propTypeExpr->getEntityVarDecl();

                if (decl->getType() == EvaluatedType::NodePattern) {
                    _currentNode->addNodeUpdate(v.propTypeExpr->getEntityVarDecl(),
                                                v.propTypeExpr->getPropName(),
                                                v.propValueExpr);

                } else if (decl->getType() == EvaluatedType::EdgePattern) {
                    _currentNode->addEdgeUpdate(v.propTypeExpr->getEntityVarDecl(),
                                                v.propTypeExpr->getPropName(),
                                                v.propValueExpr);
                }
            },

            // SymbolAddAssign case
            [this, item](const SetItem::SymbolAddAssign& v) {
                throwError("SET cannot dynamically mutate properties yet", item);
            },

            // SymbolEntityTypes case
            [this, item](const SetItem::SymbolEntityTypes& v) {
                throwError("SET cannot update entity types yet", item);
            }};

        std::visit(visitor, item->item());
    };
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
            if (_currentNode->hasPendingNode(decl) && !DELETE_PENDING_SUPPORTED) {
                // TODO: Check if we should support this, Neo4j does
                throwError("Cannot delete pending node", symbol);
            }

            _currentNode->deleteNode(decl);
        } else if (type == EvaluatedType::EdgePattern) {
            if (_currentNode->hasPendingEdge(decl) && !DELETE_PENDING_SUPPORTED) {
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

    if (!origin) [[unlikely]] {
        throwError("CREATE statement must have a node pattern as origin", element);
    }

    const VarDecl* originDecl = origin->getDecl();
    const NodePatternData* data = origin->getData();

    const VarDecl* lhs = nullptr;
    const VarDecl* rhs = nullptr;

    // A node may:
    //   - Have to be created
    //   - Be an input to the write query: MATCH (n) CREATE (n)-[e:E]->(m)
    //   - Already created in the query: CREATE (n:Person), (n)-[e:E]->(m)

    // Step 1. Handle the origin
    const VarNode* varNode = _variables->getVarNode(originDecl);

    if (!_currentNode->hasPendingNode(originDecl) && varNode == nullptr) {
        // Node is not created yet and is not an input
        checkDependencies(*_variables, data);
        _currentNode->addNode(originDecl, data);

        _variables->setProducer(originDecl, _currentNode);
    }

    lhs = originDecl;

    // Step 2. Handle the [edge-rhs] chains
    for (auto [edge, rhsNode] : element->getElementChain()) {
        rhs = rhsNode->getDecl();

        const NodePatternData* rhsData = rhsNode->getData();
        const VarNode* rhsVarNode = _variables->getVarNode(rhs);

        if (!_currentNode->hasPendingNode(rhs) && rhsVarNode == nullptr) {
            // Node is not created yet and is not an input
            checkDependencies(*_variables, rhsData);
            _currentNode->addNode(rhs, rhsData);
            _variables->setProducer(rhs, _currentNode);
        }

        // - Create the new edge
        const VarDecl* edgeDecl = edge->getDecl();
        {
            const EdgePatternData* edgeData = edge->getData();
            checkDependencies(*_variables, edgeData);
            _variables->setProducer(edgeDecl, _currentNode);
        }

        switch (edge->getDirection()) {
            case EdgePattern::Direction::Undirected:
                throwError("Cannot use undirected edges in write statements", edge);
            case EdgePattern::Direction::Backward:
                _currentNode->addEdge(edgeDecl, edge->getData(), rhs, lhs);
                break;
            case EdgePattern::Direction::Forward:
                _currentNode->addEdge(edgeDecl, edge->getData(), lhs, rhs);
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
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
