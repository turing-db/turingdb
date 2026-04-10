#include "WriteStmtGenerator.h"

#include <string_view>
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

#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "decl/PatternData.h"

#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"

#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/WriteNode.h"
#include "nodes/VarNode.h"
#include "nodes/ExprEvalNode.h"

#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"
#include "stmt/Stmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/DeleteStmt.h"

#include "BioAssert.h"

using namespace db;

WriteStmtGenerator::WriteStmtGenerator(const CypherAST* ast,
                                       PlanGraph* tree,
                                       PlanGraphVariables* variables,
                                       PlanGraphNode* prevNode)
    : _ast(ast),
    _tree(tree),
    _variables(variables),
    _prevNode(prevNode),
    _propCache(tree->getGetPropertyCache())
{
}

WriteStmtGenerator::~WriteStmtGenerator() {
}

WriteNode* WriteStmtGenerator::generateStmt(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::CREATE:
            generateCreateStmt(static_cast<const CreateStmt*>(stmt));
        break;

        case Stmt::Kind::SET:
            generateSetStmt(static_cast<const SetStmt*>(stmt));
        break;

        case Stmt::Kind::DELETE:
            generateDeleteStmt(static_cast<const DeleteStmt*>(stmt));
        break;

        default:
            throwError(fmt::format("Unsupported write statement type: {}", (uint64_t)stmt->getKind()), stmt);
        break;
    }

    return _writeNode;
}

void WriteStmtGenerator::generateCreateStmt(const CreateStmt* stmt) {
    prepareWriteNode();

    const Pattern* pattern = stmt->getPattern();

    for (const PatternElement* element : pattern->elements()) {
        generateCreatePatternElement(element);
    }
}

void WriteStmtGenerator::generateSetStmt(const SetStmt* stmt) {
    // Always create a new write node: allows for CREATE ... SET ... to reference freshly
    // created nodes/edges. Connect it to prev node output if it exists
    if (!_prevNode) {
        _writeNode = _tree->create<WriteNode>();
    } else {
        _writeNode = _tree->newOut<WriteNode>(_prevNode);
    }

    for (const SetItem* item : stmt->getItems()) {
        const auto visitor = Overloaded {
            [this](const SetItem::PropertyExprAssign& v) {          // *n.age  =  10*
                const PropertyExpr* propertyExpr = v._propTypeExpr; // *n.age* =  10
                Expr* propertyValue = v._propValueExpr;             //  n.age  = *10*

                const VarDecl* propertyVar = propertyExpr->getEntityVarDecl();
                const std::string_view propertyName = propertyExpr->getPropName();
                const EvaluatedType propertyType = propertyVar->getType();

                ExprDependencies deps;
                deps.genExprDependencies(*_variables, propertyValue);

                // TODO: Calculate gracefully according to expr tree.
                for (auto& d : deps.getVarDeps()) {
                    Expr* e = d._expr;
                    if (e->getKind() == Expr::Kind::PROPERTY) {
                        fetchOrGenerateProperty(static_cast<PropertyExpr*>(e));
                    }
                }

                if (propertyType == EvaluatedType::NodePattern) {
                    _writeNode->addNodeUpdate(propertyVar, propertyName, propertyValue);

                } else if (propertyType == EvaluatedType::EdgePattern) {
                    _writeNode->addEdgeUpdate(propertyVar, propertyName, propertyValue);
                }
            },

            // SymbolAddAssign case
            // e.g. n.age += 10 (?)
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

void WriteStmtGenerator::generateDeleteStmt(const DeleteStmt* stmt) {
    prepareWriteNode();

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
            if (_writeNode->hasPendingNode(decl) && !DELETE_PENDING_SUPPORTED) {
                // TODO: Check if we should support this, Neo4j does
                throwError("Cannot delete pending node", symbol);
            }

            _writeNode->deleteNode(decl);
        } else if (type == EvaluatedType::EdgePattern) {
            if (_writeNode->hasPendingEdge(decl) && !DELETE_PENDING_SUPPORTED) {
                // TODO: Check if we should support this, Neo4j does
                throwError("Cannot delete pending edge", symbol);
            }

            _writeNode->deleteEdge(decl);
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

    if (!_writeNode->hasPendingNode(originDecl) && varNode == nullptr) {
        // Node is not created yet and is not an input
        supplyPropertyDependencies(*_variables, data);
        _writeNode->addNode(originDecl, data);

        _variables->setProducer(originDecl, _writeNode);
    }

    lhs = originDecl;

    // Step 2. Handle the [edge-rhs] chains
    for (auto [edge, rhsNode] : element->getElementChain()) {
        rhs = rhsNode->getDecl();

        const NodePatternData* rhsData = rhsNode->getData();
        const VarNode* rhsVarNode = _variables->getVarNode(rhs);

        if (!_writeNode->hasPendingNode(rhs) && rhsVarNode == nullptr) {
            // Node is not created yet and is not an input
            supplyPropertyDependencies(*_variables, rhsData);
            _writeNode->addNode(rhs, rhsData);
            _variables->setProducer(rhs, _writeNode);
        }

        // - Create the new edge
        const VarDecl* edgeDecl = edge->getDecl();
        {
            const EdgePatternData* edgeData = edge->getData();
            supplyPropertyDependencies(*_variables, edgeData);
            _variables->setProducer(edgeDecl, _writeNode);
        }

        switch (edge->getDirection()) {
            case EdgePattern::Direction::Undirected:
                throwError("Cannot use undirected edges in write statements", edge);
            case EdgePattern::Direction::Backward:
                _writeNode->addEdge(edgeDecl, edge->getData(), rhs, lhs);
                break;
            case EdgePattern::Direction::Forward:
                _writeNode->addEdge(edgeDecl, edge->getData(), lhs, rhs);
                break;
        }

        lhs = rhs;
    }
}

void WriteStmtGenerator::prepareWriteNode() {
    if (!_prevNode) {
        // First node in the plan graph
        _writeNode = _tree->create<WriteNode>();
    } else if (_prevNode->getOpcode() == PlanGraphOpcode::WRITE) {
        // Previous node is a write node, reuse it
        _writeNode = static_cast<WriteNode*>(_prevNode);
    } else {
        // Previous node is not a write node, create a new one
        _writeNode = _tree->newOut<WriteNode>(_prevNode);
    }
}

void WriteStmtGenerator::supplyPropertyDependencies(PlanGraphVariables& variables,
                                                    const PatternData* data) {
    ExprDependencies deps;
    for (const EntityPropertyConstraint& d : data->exprConstraints()) {
        deps.genExprDependencies(variables, d._expr);

        for (const ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
            Expr* e = dep._expr;
            bioassert(e, "Null dependency.");

            if (e->getKind() == Expr::Kind::PROPERTY) {
                fetchOrGenerateProperty(static_cast<PropertyExpr*>(e));
            } else {
                throwError("Unknown dependency.", e);
            }
        }
    }
}

void WriteStmtGenerator::fetchOrGenerateProperty(PropertyExpr* prop) {
    const VarDecl* entityVar = prop->getEntityVarDecl();
    const VarDecl* propVar = prop->getExprVarDecl();
    const std::string_view propName = prop->getPropName();

    if (!propVar) {
        throwError("Property had no variable declaration.", prop);
    }

    if (!entityVar) {
        throwError("Property had no enitity variable declation.", prop);
    }

    const auto* cached = _propCache.cacheOrRetrieve(entityVar, propVar, propName);
    if (cached) {
        if (!cached->_exprDecl) {
            throwError("Cached property had no expression variable.", prop);
        }
        // GetProperty is already present in the cache. Map the existing propExpr
        // to the current one
        prop->setExprVarDecl(cached->_exprDecl);
        return;
    }

    // Otherwise we need to fetch this property just for the return, add a node and update
    // the propExpr. Place the GetProperties immediately: this ensures all properties are
    // present for any expression evaluations that may use them.
    auto* getProps = _tree->insertBefore<GetPropertyWithNullNode>(_writeNode, propName);
    _prevNode = getProps;

    getProps->setExpr(prop);
    getProps->setEntityVarDecl(entityVar);
}

void WriteStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
