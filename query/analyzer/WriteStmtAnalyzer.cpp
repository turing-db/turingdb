#include "WriteStmtAnalyzer.h"

#include <spdlog/fmt/bundled/format.h>

#include "AnalyzeException.h"
#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "Overloaded.h"

#include "Literal.h"
#include "EdgePattern.h"
#include "ExprAnalyzer.h"
#include "NodePattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/PropertyExpr.h"
#include "stmt/DeleteStmt.h"
#include "stmt/SetItem.h"
#include "stmt/Stmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/SetStmt.h"
#include "stmt/RemoveStmt.h"

using namespace db;

namespace {

// A tagged cell carries its type per row, so the write checks each cell against the
// property's type as it stages it, and gives a property with no type yet the first one's
bool writeTypeCompatible(ValueType propertyType, EvaluatedType valueType) {
    return valueType == EvaluatedType::ListItem || ExprAnalyzer::propTypeCompatible(propertyType, valueType);
}

}

WriteStmtAnalyzer::WriteStmtAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(_graphView.metadata())
{
}

WriteStmtAnalyzer::~WriteStmtAnalyzer() {
}

void WriteStmtAnalyzer::analyze(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::CREATE:
            analyze(static_cast<const CreateStmt*>(stmt));
            break;

        case Stmt::Kind::MERGE:
            analyze(static_cast<const MergeStmt*>(stmt));
            break;

        case Stmt::Kind::SET:
            analyze(static_cast<const SetStmt*>(stmt));
            break;

        case Stmt::Kind::REMOVE:
            analyze(static_cast<const RemoveStmt*>(stmt));
            break;

        case Stmt::Kind::DELETE:
            analyze(static_cast<const DeleteStmt*>(stmt));
            break;

        default:
            throwError(fmt::format("Unsupported write statement type: {}",
                                   (uint64_t)stmt->getKind()),
                       stmt);
            break;
    }
}

void WriteStmtAnalyzer::analyze(const CreateStmt* createStmt) {
    if (const Pattern* pattern = createStmt->getPattern()) {
        throwOnEntityWhere(pattern, "CREATE");
        analyze(pattern);
    }
}

void WriteStmtAnalyzer::analyze(const MergeStmt* mergeStmt) {
    const Pattern* pattern = mergeStmt->getPattern();

    throwOnEntityWhere(pattern, "MERGE");
    analyze(pattern);

    for (const SetStmt* actions : {mergeStmt->getOnCreate(), mergeStmt->getOnMatch()}) {
        if (!actions) {
            continue;
        }

        for (SetItem* item : actions->getItems()) {
            analyze(item);
        }
    }
}

void WriteStmtAnalyzer::analyze(const SetStmt* setStmt) {
    for (SetItem* item : setStmt->getItems()) {
        analyze(item);
    }
}

void WriteStmtAnalyzer::analyze(const RemoveStmt* removeStmt) {
    constexpr bool ALLOW_CREATES = false;

    for (PropertyExpr* property : removeStmt->getProperties()) {
        _exprAnalyzer->analyzePropertyExpr(property, ALLOW_CREATES, ValueType::Invalid);
        _exprAnalyzer->throwIfReadsADateTimeComponent(property);
    }
}

void WriteStmtAnalyzer::analyze(const DeleteStmt* deleteStmt) {
    const ExprChain* exprs = deleteStmt->getExpressions();
    for (Expr* expr : *exprs) {
        _exprAnalyzer->analyzeRootExpr(expr);

        if (expr->isAggregate()) {
            throwError("Invalid use of aggregate expression in this context", deleteStmt);
        }
    }
}

void WriteStmtAnalyzer::analyze(const Pattern* pattern) {
    for (PatternElement* element : pattern->elements()) {
        analyze(element);
    }
}

void WriteStmtAnalyzer::throwOnEntityWhere(const Pattern* pattern, std::string_view clause) const {
    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            if (entity->getWhere()) {
                throwError(fmt::format("WHERE is not allowed in a {} pattern", clause), entity);
            }
        }
    }
}

void WriteStmtAnalyzer::analyze(PatternElement* element) {
    const auto& entities = element->getEntities();

    for (EntityPattern* entity : entities) {
        if (NodePattern* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(node);
        } else if (EdgePattern* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(edge);
        } else {
            throwError(fmt::format("Unsupported pattern entity type"), entity);
        }
    }

    analyzeNamedPath(element);
}

void WriteStmtAnalyzer::analyzeNamedPath(PatternElement* element) {
    Symbol* symbol = element->getPathSymbol();
    if (!symbol) {
        return;
    }

    const std::string_view name = symbol->getName();

    if (_ctxt->getDecl(name)) {
        throwError(fmt::format("Variable '{}' is already bound: a named path takes a name of its own", name),
                   element);
    }

    VarDecl* decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::GraphPath, name);
    element->setPathDecl(decl);
}

void WriteStmtAnalyzer::analyze(NodePattern* nodePattern) {
    if (nodePattern->getWhere()) {
        throwError("WHERE cannot be used in a write pattern", nodePattern);
    }

    VarDecl* decl = nullptr;

    if (Symbol* symbol = nodePattern->getSymbol()) {
        decl = _ctxt->getDecl(symbol->getName());
        if (decl) {
            // Node already defined. It is either created in the query, or an input to the write query
            if (decl->getType() != EvaluatedType::NodePattern) {
                throwError(fmt::format("Type mismatch. Expected NodePattern but is {} instead ",
                                       EvaluatedTypeName::value(decl->getType())),
                           nodePattern);
            }

            const auto& labels = nodePattern->labels();

            // Already existing vars cannot have constraints
            if (nodePattern->getData() != nullptr || labels != nullptr) {
                throwError("Variable already defined", nodePattern);
            }

            nodePattern->setDecl(decl);
            return;
        }
        decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::NodePattern, symbol->getName());
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::NodePattern);
    }

    _toBeCreated.insert(decl);

    NodePatternData* data = NodePatternData::create(_ast);

    nodePattern->setDecl(decl);
    nodePattern->setData(data);

    const auto& labels = nodePattern->labels();
    if (labels == nullptr) {
        throwError("Node pattern must have at least one label", nodePattern);
    }

    for (const Symbol* label : *labels) {
        data->addLabelConstraint(label->getName());
    }

    const MapLiteral* properties = nodePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyzeRootExpr(expr);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", nodePattern);
            }

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (propType) {
                // Property type already exists
                if (!writeTypeCompatible(propType->_valueType, expr->getType())) {
                    throwError(fmt::format("Cannot evaluate node property: types '{}' and '{}' are incompatible",
                                           ValueTypeName::value(propType->_valueType),
                                           EvaluatedTypeName::value(expr->getType())),
                               nodePattern);
                }
                data->addExprConstraint(propName->getName(), propType->_valueType, expr);
            } else if (expr->getType() == EvaluatedType::ListItem) {
                data->addExprConstraint(propName->getName(), ValueType::Invalid, expr);
                _exprAnalyzer->addToBeCreatedFromTaggedCells(propName->getName());
            } else {
                // Property type needs to be created
                const ValueType valueType = evaluatedToValueType(expr->getType());
                if (valueType == ValueType::Invalid) {
                    throwError(fmt::format("Cannot evaluate node property: unsupported type '{}'", EvaluatedTypeName::value(expr->getType())),
                               expr);
                }
                data->addExprConstraint(propName->getName(), valueType, expr);
                _exprAnalyzer->addToBeCreatedType(propName->getName(), valueType, expr);
            }
        }
    }
}

void WriteStmtAnalyzer::analyze(EdgePattern* edgePattern) {
    if (edgePattern->getWhere()) {
        throwError("WHERE cannot be used in a write pattern", edgePattern);
    }

    VarDecl* decl = nullptr;

    if (Symbol* symbol = edgePattern->getSymbol()) {
        decl = _ctxt->getDecl(symbol->getName());
        if (decl) {
            throwError("Edges cannot be inputs to write queries", edgePattern);
        }

        decl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::EdgePattern, symbol->getName());
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::EdgePattern);
    }

    EdgePatternData* data = EdgePatternData::create(_ast);

    edgePattern->setDecl(decl);
    edgePattern->setData(data);

    if (edgePattern->getQuantifiedPath()) {
        throwError("Variable length relationships cannot be used in a write pattern", edgePattern);
    }

    if (edgePattern->types() == nullptr) {
        throwError("Edge pattern must have at least one edge type", edgePattern);
    }

    const auto& types = edgePattern->types();

    if (types->size() > 1) {
        throwError("An edge cannot have more than one edge type", edgePattern);
    }

    data->addEdgeTypeConstraint(types->front()->getName());

    const MapLiteral* properties = edgePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyzeRootExpr(expr);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", edgePattern);
            }

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (propType) {
                // Property type already exists
                if (!writeTypeCompatible(propType->_valueType, expr->getType())) {
                    throwError(fmt::format("Cannot evaluate edge property: types '{}' and '{}' are incompatible",
                                           ValueTypeName::value(propType->_valueType),
                                           EvaluatedTypeName::value(expr->getType())),
                               edgePattern);
                }
                data->addExprConstraint(propName->getName(), propType->_valueType, expr);
            } else if (expr->getType() == EvaluatedType::ListItem) {
                data->addExprConstraint(propName->getName(), ValueType::Invalid, expr);
                _exprAnalyzer->addToBeCreatedFromTaggedCells(propName->getName());
            } else {
                // Property type needs to be created
                const ValueType valueType = evaluatedToValueType(expr->getType());
                if (valueType == ValueType::Invalid) {
                    throwError(fmt::format("Cannot evaluate edge property: unsupported type '{}'", EvaluatedTypeName::value(expr->getType())),
                               expr);
                }
                data->addExprConstraint(propName->getName(), valueType, expr);
                _exprAnalyzer->addToBeCreatedType(propName->getName(), valueType, expr);
            }
        }
    }
}

void WriteStmtAnalyzer::analyze(SetItem* item) {
    const auto visitor = Overloaded {
        // PropertyExprAssign case, e.g;
        // MATCH (n) SET n.age = 10
        // MATCH (n), (m) SET n.age = m.age
        [this, item](const SetItem::PropertyExprAssign& v) {
            PropertyExpr* lhs = v._propTypeExpr;
            Expr* rhs = v._propValueExpr;

            _exprAnalyzer->analyzeExpr(rhs);
            const EvaluatedType rhsType = rhs->getType();

            // writing null cannot create a new property
            const bool writesNull = rhsType == EvaluatedType::Null;
            const bool allowCreates = !writesNull;

            if (rhsType == EvaluatedType::ListItem) {
                const QualifiedName* propertyName = lhs->getFullName();
                _exprAnalyzer->addToBeCreatedFromTaggedCells(propertyName->back()->getName());
            }

            const ValueType valType = evaluatedToValueType(rhsType);
            const ValueType lhsEvaluatedVt =
                _exprAnalyzer->analyzePropertyExpr(lhs, allowCreates, valType);

            _exprAnalyzer->throwIfReadsADateTimeComponent(lhs);

            _exprAnalyzer->analyzeRootExpr(v._propValueExpr);

            if (rhs->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", item);
            }

            // An element of a mixed list carries its type per row, so the write checks it
            // against the property's type
            const bool writesAListElement = rhsType == EvaluatedType::ListItem;
            const bool propertyHasAType = lhsEvaluatedVt != ValueType::Invalid;
            const bool checksTheTypePerRow = writesAListElement && propertyHasAType;

            if (writesNull || checksTheTypePerRow) {
                return;
            }

            if (!writeTypeCompatible(lhsEvaluatedVt, rhsType)) {
                throwError(fmt::format("Cannot evaluate property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(lhsEvaluatedVt),
                                       EvaluatedTypeName::value(rhsType)),
                           item);
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
}

void WriteStmtAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}

db::ValueType WriteStmtAnalyzer::evaluatedToValueType(EvaluatedType type) {
    switch (type) {
        case EvaluatedType::Bool:
            return ValueType::Bool;
        case EvaluatedType::Char:
        case EvaluatedType::String:
            return ValueType::String;
        case EvaluatedType::Double:
            return ValueType::Double;
        case EvaluatedType::Integer:
            return ValueType::Int64;
        case EvaluatedType::Embedding:
            return ValueType::Embedding;
        case EvaluatedType::List:
            return ValueType::List;
        case EvaluatedType::DateTime:
            return ValueType::DateTime;
        case EvaluatedType::Map:
            return ValueType::Map;
        case EvaluatedType::Null:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
        case EvaluatedType::StringTable:
        case EvaluatedType::Invalid:
        case EvaluatedType::Wildcard:
        case EvaluatedType::GraphPath:
        case EvaluatedType::Tuple:
        case EvaluatedType::ValueType:
        case EvaluatedType::Label:
        case EvaluatedType::LabelSet:
        case EvaluatedType::PropertyType:
        case EvaluatedType::EdgeType:
        case EvaluatedType::_SIZE:
        case EvaluatedType::ListItem:
            return ValueType::Invalid;
        break;
    }

    return ValueType::Invalid;
}

