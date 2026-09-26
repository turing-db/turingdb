#include "WriteStmtAnalyzer.h"

#include <unordered_set>

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
#include "expr/EntityTypeExpr.h"
#include "expr/ExistsExpr.h"
#include "expr/ExprChildren.h"
#include "expr/PatternComprehensionExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "Projection.h"
#include "SinglePartQuery.h"
#include "WhereClause.h"
#include "stmt/DeleteStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"
#include "stmt/UnwindStmt.h"
#include "stmt/WithStmt.h"
#include "stmt/SetItem.h"
#include "stmt/Stmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/SetStmt.h"
#include "stmt/RemoveStmt.h"

using namespace db;

namespace {

const MapLiteral* mapLiteralOf(const Expr* expr) {
    if (expr->getKind() != Expr::Kind::LITERAL) {
        return nullptr;
    }

    const Literal* literal = static_cast<const LiteralExpr*>(expr)->getLiteral();
    if (literal->getKind() != Literal::Kind::MAP) {
        return nullptr;
    }

    return static_cast<const MapLiteral*>(literal);
}

using NameSet = std::unordered_set<std::string_view>;

std::string_view findReadName(const Expr* expr, const NameSet& names);

std::string_view findPatternReadName(const Pattern* pattern, const NameSet& names) {
    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const Symbol* symbol = entity->getSymbol();
            if (symbol && names.contains(symbol->getName())) {
                return symbol->getName();
            }

            if (const MapLiteral* properties = entity->getProperties()) {
                for (const auto& [key, value] : *properties) {
                    const std::string_view found = findReadName(value, names);
                    if (!found.empty()) {
                        return found;
                    }
                }
            }

            if (const WhereClause* where = entity->getWhere()) {
                const std::string_view found = findReadName(where->getExpr(), names);
                if (!found.empty()) {
                    return found;
                }
            }
        }
    }

    const WhereClause* where = pattern->getWhere();
    return where ? findReadName(where->getExpr(), names) : std::string_view {};
}

std::string_view findProjectionReadName(const Projection* projection, const NameSet& names) {
    for (const Projection::ReturnItem& item : projection->items()) {
        const Expr* const* itemExpr = std::get_if<Expr*>(&item);
        const std::string_view found = itemExpr ? findReadName(*itemExpr, names) : std::string_view {};

        if (!found.empty()) {
            return found;
        }
    }

    return {};
}

std::string_view findQueryReadName(const SinglePartQuery* query, const NameSet& names) {
    std::string_view found;

    if (const StmtContainer* stmts = query->getStmts()) {
        for (const Stmt* stmt : stmts->stmts()) {
            const Stmt::Kind kind = stmt->getKind();

            if (kind == Stmt::Kind::MATCH) {
                found = findPatternReadName(static_cast<const MatchStmt*>(stmt)->getPattern(), names);
            } else if (kind == Stmt::Kind::UNWIND) {
                found = findReadName(static_cast<const UnwindStmt*>(stmt)->arg(), names);
            } else if (kind == Stmt::Kind::WITH) {
                const WithStmt* with = static_cast<const WithStmt*>(stmt);
                found = findProjectionReadName(with->getProjection(), names);

                const WhereClause* where = with->getWhere();
                if (found.empty() && where) {
                    found = findReadName(where->getExpr(), names);
                }
            }

            if (!found.empty()) {
                return found;
            }
        }
    }

    const ReturnStmt* returnStmt = query->getReturnStmt();
    return returnStmt ? findProjectionReadName(returnStmt->getProjection(), names) : std::string_view {};
}

// A name of @param names that @param expr reads, or an empty one. It goes by name: an EXISTS
// reads the variables around it through declarations of its own.
std::string_view findReadName(const Expr* expr, const NameSet& names) {
    const Expr::Kind kind = expr->getKind();

    const VarDecl* read = nullptr;
    if (kind == Expr::Kind::PROPERTY) {
        read = static_cast<const PropertyExpr*>(expr)->getEntityVarDecl();
    } else if (kind == Expr::Kind::SYMBOL) {
        read = static_cast<const SymbolExpr*>(expr)->getDecl();
    } else if (kind == Expr::Kind::ENTITY_TYPES) {
        read = static_cast<const EntityTypeExpr*>(expr)->getEntityVarDecl();
    } else if (kind == Expr::Kind::EXISTS) {
        const ExistsExpr* exists = static_cast<const ExistsExpr*>(expr);

        for (const SinglePartQuery* branch : exists->branches()) {
            const std::string_view found = findQueryReadName(branch, names);
            if (!found.empty()) {
                return found;
            }
        }

        return {};
    } else if (kind == Expr::Kind::PATTERN_COMPREHENSION) {
        const PatternComprehensionExpr* comprehension = static_cast<const PatternComprehensionExpr*>(expr);

        const std::string_view found = findPatternReadName(comprehension->getPattern(), names);
        return found.empty() ? findReadName(comprehension->getProjection(), names) : found;
    }

    if (read && names.contains(read->getName())) {
        return read->getName();
    }

    std::vector<const Expr*> children;
    ExprChildren::collect(expr, children);

    for (const Expr* child : children) {
        const std::string_view found = findReadName(child, names);
        if (!found.empty()) {
            return found;
        }
    }

    return {};
}

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
        throwOnUndirectedEdge(pattern);

        _patternClause = "CREATE";
        collectPatternNames(pattern);
        analyze(pattern);
    }
}

void WriteStmtAnalyzer::analyze(const MergeStmt* mergeStmt) {
    const Pattern* pattern = mergeStmt->getPattern();

    throwOnEntityWhere(pattern, "MERGE");

    _patternClause = "MERGE";
    collectPatternNames(pattern);
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

void WriteStmtAnalyzer::collectPatternNames(const Pattern* pattern) {
    _patternNames.clear();

    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const Symbol* symbol = entity->getSymbol();
            if (symbol && !_ctxt->getDecl(symbol->getName())) {
                _patternNames.insert(symbol->getName());
            }
        }
    }
}

void WriteStmtAnalyzer::throwOnPatternEntityRead(const Expr* expr, const void* obj) const {
    const std::string_view read = findReadName(expr, _patternNames);
    if (read.empty()) {
        return;
    }

    throwError(fmt::format("A property of this {} reads '{}', which the same {} introduces: it has no value "
                           "until the clause has run, so only variables bound by an earlier clause can be read here",
                           _patternClause,
                           read,
                           _patternClause),
               obj);
}

void WriteStmtAnalyzer::throwOnUndirectedEdge(const Pattern* pattern) const {
    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const EdgePattern* edge = dynamic_cast<const EdgePattern*>(entity);

            if (edge && edge->getDirection() == EdgePattern::Direction::Undirected) {
                throwError("Only directed relationships are supported in CREATE", entity);
            }
        }
    }
}

void WriteStmtAnalyzer::analyze(PatternElement* element) {
    const auto& entities = element->getEntities();

    const NodePattern* soleNode = entities.size() == 1 ? dynamic_cast<const NodePattern*>(entities.front()) : nullptr;
    const bool isBareNode = soleNode && soleNode->getSymbol() && !soleNode->labels() && !soleNode->getProperties();
    if (isBareNode && _ctxt->getDecl(soleNode->getSymbol()->getName())) {
        throwError(fmt::format("Variable '{}' already declared", soleNode->getSymbol()->getName()), soleNode);
    }

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
            if (nodePattern->getData() != nullptr || labels != nullptr || nodePattern->getProperties() != nullptr) {
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
            throwOnPatternEntityRead(expr, nodePattern);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", nodePattern);
            }

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (expr->getType() == EvaluatedType::Null) {
                data->addExprConstraint(propName->getName(), ValueType::Invalid, expr);
            } else if (propType) {
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
        if (decl && _patternNames.contains(symbol->getName())) {
            throwError(fmt::format("Relationship variable '{}' appears twice in this pattern, and each "
                                   "relationship of a pattern is an edge of its own",
                                   symbol->getName()),
                       edgePattern);
        } else if (decl) {
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
            throwOnPatternEntityRead(expr, edgePattern);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", edgePattern);
            }

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (expr->getType() == EvaluatedType::Null) {
                data->addExprConstraint(propName->getName(), ValueType::Invalid, expr);
            } else if (propType) {
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
            analyzePropertyAssign(item, v._propTypeExpr, v._propValueExpr);
        },

        // SymbolMapAssign case, e.g;
        // MATCH (n) SET n += {age: 10}
        // MATCH (n) SET n = {name: 'x'}
        [this, item](SetItem::SymbolMapAssign& v) {
            analyzeMapAssign(item, v);
        },

        // SymbolEntityTypes case
        [this, item](const SetItem::SymbolEntityTypes& v) {
            throwError("SET cannot update entity types yet", item);
        }};

    std::visit(visitor, item->item());
}

void WriteStmtAnalyzer::analyzePropertyAssign(const SetItem* item, PropertyExpr* lhs, Expr* rhs) {
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

    _exprAnalyzer->analyzeRootExpr(rhs);

    if (rhs->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", item);
    }

    const bool writesThroughNull = lhs->getEntityVarDecl()->getType() == EvaluatedType::Null;
    if (writesNull || writesThroughNull) {
        return;
    }

    if (!writeTypeCompatible(lhsEvaluatedVt, rhsType)) {
        throwError(fmt::format("Cannot evaluate property: types '{}' and '{}' are incompatible",
                               ValueTypeName::value(lhsEvaluatedVt),
                               EvaluatedTypeName::value(rhsType)),
                   item);
    }
}

void WriteStmtAnalyzer::analyzeMapAssign(const SetItem* item, SetItem::SymbolMapAssign& assign) {
    const std::string_view varName = assign._symbol->getName();

    VarDecl* decl = _ctxt->getDecl(varName);
    if (!decl) {
        throwError(fmt::format("Variable '{}' not found", varName), item);
    }

    const EvaluatedType varType = decl->getType();
    const bool writesAnEntity = varType == EvaluatedType::NodePattern || varType == EvaluatedType::EdgePattern;
    if (!writesAnEntity && varType != EvaluatedType::Null) {
        throwError(fmt::format("Variable '{}' is '{}', and SET writes the properties of a node or an edge",
                               varName,
                               EvaluatedTypeName::value(varType)),
                   item);
    }

    assign._decl = decl;

    const MapLiteral* map = mapLiteralOf(assign._value);
    if (!map) {
        analyzeComputedValue(item, assign);
        return;
    }

    std::unordered_set<std::string_view> keys;
    for (const auto& [key, value] : *map) {
        QualifiedName* propertyName = QualifiedName::create(_ast);
        propertyName->addName(assign._symbol);
        propertyName->addName(key);

        PropertyExpr* propertyExpr = PropertyExpr::create(_ast, propertyName);
        assign._entries.push_back({propertyExpr, value});
        keys.insert(key->getName());

        analyzePropertyAssign(item, propertyExpr, value);
    }

    if (!assign._replaces) {
        return;
    }

    const auto removeUnlessAKey = [&assign, &keys](std::string_view name) {
        if (!keys.contains(name)) {
            assign._removedProperties.push_back(name);
        }
    };

    for (const PropertyTypeMap::Pair& property : _graphMetadata.propTypes()) {
        removeUnlessAKey(*property._name);
    }

    for (const auto& [name, valueType] : _exprAnalyzer->getToBeCreatedTypes()) {
        removeUnlessAKey(name);
    }

    for (const std::string_view name : _exprAnalyzer->getToBeCreatedFromTaggedCells()) {
        removeUnlessAKey(name);
    }
}

// SET n = m and SET n += m, m a node or an edge: every property the query knows of, read
// off m and written to n
void WriteStmtAnalyzer::analyzeComputedValue(const SetItem* item, SetItem::SymbolMapAssign& assign) {
    Expr* value = assign._value;
    _exprAnalyzer->analyzeExpr(value);
    _exprAnalyzer->analyzeRootExpr(value);

    if (value->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", item);
    }

    const EvaluatedType valueType = value->getType();
    const bool readsAnEntity = valueType == EvaluatedType::NodePattern || valueType == EvaluatedType::EdgePattern;

    const bool readsAMap = valueType == EvaluatedType::Map
                        || valueType == EvaluatedType::Null
                        || valueType == EvaluatedType::ListItem;

    if (readsAMap) {
        assign._writesRowEntries = true;
    } else if (readsAnEntity) {
        analyzeEntityCopy(item, assign);
    } else {
        throwError(fmt::format("SET writes a map of properties, not '{}'", EvaluatedTypeName::value(valueType)), item);
    }
}

void WriteStmtAnalyzer::analyzeEntityCopy(const SetItem* item, SetItem::SymbolMapAssign& assign) {
    Expr* value = assign._value;
    const EvaluatedType valueType = value->getType();

    assign._copiesEntity = true;

    Symbol* source = nullptr;
    if (value->getKind() == Expr::Kind::SYMBOL) {
        source = static_cast<SymbolExpr*>(value)->getSymbol();
    } else {
        assign._sourceDecl = _ctxt->createUnnamedVariable(_ast, valueType);
        source = Symbol::create(_ast, assign._sourceDecl->getName());
    }
    std::unordered_set<std::string_view> copied;

    const auto copy = [&](std::string_view name) {
        if (!copied.insert(name).second) {
            return;
        }

        Symbol* property = Symbol::create(_ast, name);

        QualifiedName* targetName = QualifiedName::create(_ast);
        targetName->addName(assign._symbol);
        targetName->addName(property);

        QualifiedName* sourceName = QualifiedName::create(_ast);
        sourceName->addName(source);
        sourceName->addName(property);

        PropertyExpr* target = PropertyExpr::create(_ast, targetName);
        PropertyExpr* read = PropertyExpr::create(_ast, sourceName);
        read->setEntityVarDecl(assign._sourceDecl);
        assign._entries.push_back({target, read});

        analyzePropertyAssign(item, target, read);
    };

    for (const PropertyTypeMap::Pair& property : _graphMetadata.propTypes()) {
        copy(*property._name);
    }

    for (const auto& [name, createdType] : _exprAnalyzer->getToBeCreatedTypes()) {
        copy(name);
    }

    for (const std::string_view name : _exprAnalyzer->getToBeCreatedFromTaggedCells()) {
        copy(name);
    }
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

