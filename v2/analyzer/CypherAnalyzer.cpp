#include "CypherAnalyzer.h"

#include <spdlog/fmt/bundled/core.h>

#include "AnalyzeException.h"
#include "CypherError.h"
#include "CypherAST.h"

#include "attribution/PatternData.h"
#include "attribution/DeclContext.h"
#include "attribution/VarDecl.h"
#include "attribution/EvaluatedType.h"

#include "expressions/All.h"

#include "types/SinglePartQuery.h"
#include "types/Projection.h"

#include "statements/Match.h"
#include "statements/Skip.h"
#include "statements/Limit.h"
#include "statements/Return.h"

using namespace db;

CypherAnalyzer::CypherAnalyzer(CypherAST& ast,
                               GraphView graphView)
    : _ast(&ast),
      _graphView(graphView),
      _graphMetadata(graphView.metadata()) {
}

CypherAnalyzer::~CypherAnalyzer() = default;

void CypherAnalyzer::analyze() {
    for (const auto& query : _ast->queries()) {
        _ctxt = &query->getRootContext();

        if (const auto* q = dynamic_cast<SinglePartQuery*>(query.get())) {
            analyze(*q);
        } else {
            throwError("Unsupported query type", query.get());
        }
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery& query) {
    for (const auto* statement : query.getStatements()) {
        if (const auto* s = dynamic_cast<const Match*>(statement)) {
            analyze(*s);
        } else if (const auto* s = dynamic_cast<const Return*>(statement)) {
            analyze(*s);
        } else {
            throwError("Unsupported statement type", statement);
        }
    }
}

void CypherAnalyzer::analyze(const Match& matchSt) {
    if (matchSt.isOptional()) {
        throwError("OPTIONAL MATCH not supported", &matchSt);
    }

    if (!matchSt.hasPattern()) {
        throwError("MATCH statement must have a pattern", &matchSt);
    }

    const auto& pattern = matchSt.getPattern();
    analyze(pattern);

    if (matchSt.hasLimit()) {
        throwError("LIMIT not supported", &matchSt);
    }

    if (matchSt.hasSkip()) {
        throwError("SKIP not supported", &matchSt);
    }
}

void CypherAnalyzer::analyze(const Skip& skipSt) {
    throwError("SKIP not supported", &skipSt);
}

void CypherAnalyzer::analyze(const Limit& limitSt) {
    throwError("LIMIT not supported", &limitSt);
}

void CypherAnalyzer::analyze(const Return& returnSt) {
    const auto& projection = returnSt.getProjection();
    if (projection.isDistinct()) {
        throwError("DISTINCT not supported", &returnSt);
    }

    if (projection.hasSkip()) {
        throwError("SKIP not supported", &returnSt);
    }

    if (projection.hasLimit()) {
        throwError("LIMIT not supported", &returnSt);
    }

    if (projection.isAll()) {
        return;
    }

    for (Expression* item : projection.items()) {
        analyze(*item);
    }
}

void CypherAnalyzer::analyze(const Pattern& pattern) {
    for (const auto& element : pattern.elements()) {
        analyze(*element);
    }

    if (pattern.hasWhere()) {
        auto& whereExpr = pattern.getWhere().getExpression();
        analyze(whereExpr);

        if (whereExpr.type() != EvaluatedType::Bool) {
            throwError("WHERE expression must be a boolean", &pattern);
        }
    }
}

void CypherAnalyzer::analyze(const PatternElement& element) {
    const auto& entities = element.getEntities();

    for (const auto& entity : entities) {
        if (auto* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(*node);
        } else if (auto* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(*edge);
        } else {
            throwError("Unsupported pattern entity type", entity);
        }
    }
}

void CypherAnalyzer::analyze(NodePattern& node) {
    if (node.hasSymbol()) {
        VarDecl& decl = _ctxt->getOrCreateNamedVariable(EvaluatedType::NodePattern, node.symbol()._name);
        node.setDecl(&decl);
    }

    auto& data = _ast->newAnalysisData<NodePatternData>(EvaluatedType::NodePattern);
    node.setData(&data);

    if (node.hasLabels()) {
        const auto& labelMap = _graphMetadata.labels();

        for (const auto& label : node.labels()) {
            const std::optional<LabelID> labelID = labelMap.get(label);

            if (!labelID) {
                throwError(fmt::format("Unknown label: {}", label), &node);
            }

            data._labelConstraints.set(labelID.value());
        }
    }

    constexpr auto compatible = [](ValueType vt, EvaluatedType exprType) {
        switch (exprType) {
            case EvaluatedType::Null:
            case EvaluatedType::NodePattern:
            case EvaluatedType::EdgePattern:
                return false;
            case EvaluatedType::Integer:
                return vt == ValueType::Int64 || vt == ValueType::UInt64 || vt == ValueType::Double;
            case EvaluatedType::Double:
                return vt == ValueType::Double;
            case EvaluatedType::String:
            case EvaluatedType::Char:
                return vt == ValueType::String;
            case EvaluatedType::Bool:
                return vt == ValueType::Bool;
            case EvaluatedType::List:
            case EvaluatedType::Map:
                return false;
            case EvaluatedType::Invalid:
            case EvaluatedType::_SIZE:
                throw AnalyzeException("Invalid variable type");
        }

        return false;
    };

    if (node.hasProperties()) {
        const auto& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : node.properties()) {
            const std::optional<PropertyType> propType = propTypeMap.get(propName);

            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName), &node);
            }

            data._exprConstraints.emplace_back(propType.value(), expr);

            if (!compatible(propType->_valueType, expr->type())) {
                throwError(fmt::format("Cannot evaluate node property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->type())),
                           &node);
            }
        }
    }
}

void CypherAnalyzer::analyze(EdgePattern& edge) {
    if (edge.hasSymbol()) {
        VarDecl& decl = _ctxt->getOrCreateNamedVariable(EvaluatedType::EdgePattern, edge.symbol()._name);
        edge.setDecl(&decl);
    }

    auto& data = _ast->newAnalysisData<EdgePatternData>(EvaluatedType::EdgePattern);
    edge.setData(&data);

    if (edge.hasTypes()) {
        const auto& edgeTypeMap = _graphMetadata.edgeTypes();

        for (const auto& et : edge.types()) {
            const std::optional<EdgeTypeID> etID = edgeTypeMap.get(et);

            if (!etID) {
                throwError(fmt::format("Unknown edge type: {}", et), &edge);
            }

            data._edgeTypeConstraints.push_back(etID.value());
        }
    }

    constexpr auto compatible = [](ValueType vt, EvaluatedType exprType) {
        switch (exprType) {
            case EvaluatedType::Null:
            case EvaluatedType::NodePattern:
            case EvaluatedType::EdgePattern:
                return false;
            case EvaluatedType::Integer:
                return vt == ValueType::Int64 || vt == ValueType::UInt64 || vt == ValueType::Double;
            case EvaluatedType::Double:
                return vt == ValueType::Double;
            case EvaluatedType::String:
            case EvaluatedType::Char:
                return vt == ValueType::String;
            case EvaluatedType::Bool:
                return vt == ValueType::Bool;
            case EvaluatedType::List:
            case EvaluatedType::Map:
                return false;
            case EvaluatedType::Invalid:
            case EvaluatedType::_SIZE:
                throw AnalyzeException("Invalid variable type");
        }

        return false;
    };

    if (edge.hasProperties()) {
        const auto& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : edge.properties()) {
            const std::optional<PropertyType> propType = propTypeMap.get(propName);

            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName), &edge);
            }

            data._exprConstraints.emplace_back(propType.value(), expr);

            if (!compatible(propType->_valueType, expr->type())) {
                throwError(fmt::format("Cannot evaluate edge property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->type())),
                           &edge);
            }
        }
    }
}

void CypherAnalyzer::analyze(Expression& expr) {
    switch (expr.kind()) {
        case ExpressionType::Binary:
            analyze(*expr.as<BinaryExpression>());
            break;
        case ExpressionType::Unary:
            analyze(*expr.as<UnaryExpression>());
            break;
        case ExpressionType::String:
            analyze(*expr.as<StringExpression>());
            break;
        case ExpressionType::NodeLabel:
            analyze(*expr.as<NodeLabelExpression>());
            break;
        case ExpressionType::Property:
            analyze(*expr.as<PropertyExpression>());
            break;
        case ExpressionType::Path:
            analyze(*expr.as<PathExpression>());
            break;
        case ExpressionType::Symbol:
            analyze(*expr.as<SymbolExpression>());
            break;
        case ExpressionType::Literal:
            analyze(*expr.as<LiteralExpression>());
            break;
        case ExpressionType::Parameter:
            analyze(*expr.as<ParameterExpression>());
            break;
    }
}

void CypherAnalyzer::analyze(BinaryExpression& expr) {
    auto& lhs = expr.left();
    auto& rhs = expr.right();

    analyze(lhs);
    analyze(rhs);

    const EvaluatedType lhsType = lhs.type();
    const EvaluatedType rhsType = rhs.type();

    EvaluatedType type = EvaluatedType::Invalid;

    constexpr auto listOrMap = [](EvaluatedType a, EvaluatedType b) {
        return a == EvaluatedType::List
            || a == EvaluatedType::Map
            || b == EvaluatedType::List
            || b == EvaluatedType::Map;
    };

    constexpr auto compatible = [](EvaluatedType a, EvaluatedType b) {
        return a == b
            || (a == EvaluatedType::Integer && b == EvaluatedType::Double)
            || (a == EvaluatedType::Double && b == EvaluatedType::Integer)
            || (a == EvaluatedType::String && b == EvaluatedType::Char)
            || (a == EvaluatedType::Char && b == EvaluatedType::String);
    };

    switch (expr.getBinaryOperator()) {
        case BinaryOperator::Or:
        case BinaryOperator::Xor:
        case BinaryOperator::And: {
            // Binary operation
            if (lhsType != EvaluatedType::Bool || rhsType != EvaluatedType::Bool) {
                throw AnalyzeException("Operands must be booleans");
            }

            type = EvaluatedType::Bool;
        } break;

        case BinaryOperator::NotEqual:
        case BinaryOperator::Equal:
        case BinaryOperator::LessThan:
        case BinaryOperator::GreaterThan:
        case BinaryOperator::LessThanOrEqual:
        case BinaryOperator::GreaterThanOrEqual: {
            if (listOrMap(lhsType, rhsType)) {
                throw AnalyzeException("Lists and maps cannot be compared");
            }

            if (!compatible(lhsType, rhsType)) {
                std::string error = fmt::format("Operands have incompatible types: '{}' and '{}'",
                                                EvaluatedTypeName::value(lhsType),
                                                EvaluatedTypeName::value(rhsType));
                throwError(std::move(error), &expr);
            }

            type = EvaluatedType::Bool;
        } break;

        case BinaryOperator::Add:
        case BinaryOperator::Sub:
        case BinaryOperator::Mult:
        case BinaryOperator::Div:
        case BinaryOperator::Mod:
        case BinaryOperator::Pow: {
            if (!compatible(lhsType, rhsType)) {
                std::string error = fmt::format("Operands have incompatible types: '{}' and '{}'",
                                                EvaluatedTypeName::value(lhsType),
                                                EvaluatedTypeName::value(rhsType));
                throwError(std::move(error), &expr);
            }

            if (lhsType == EvaluatedType::Double || rhsType == EvaluatedType::Double) {
                type = EvaluatedType::Double;
            } else if (lhsType == EvaluatedType::String || rhsType == EvaluatedType::String) {
                type = EvaluatedType::String;
            } else {
                type = lhsType;
            }

        } break;

        case BinaryOperator::In: {
            if (rhsType != EvaluatedType::List) {
                throw AnalyzeException("IN operand must be a list or map");
            }

            if (lhsType == EvaluatedType::List || lhsType == EvaluatedType::Map) {
                throw AnalyzeException("Left operand must be a scalar");
            }

            type = EvaluatedType::Bool;
        } break;
    }

    expr.setType(type);
}

void CypherAnalyzer::analyze(UnaryExpression& expr) {
    auto& operand = expr.right();
    analyze(operand);

    EvaluatedType type = EvaluatedType::Invalid;

    switch (expr.getUnaryOperator()) {
        case UnaryOperator::Not: {
            if (operand.type() != EvaluatedType::Bool) {
                throw AnalyzeException("NOT operand must be a boolean");
            }

            type = EvaluatedType::Bool;
        } break;

        case UnaryOperator::Minus:
        case UnaryOperator::Plus: {
            if (operand.type() == EvaluatedType::Integer) {
                type = EvaluatedType::Integer;
            } else if (operand.type() == EvaluatedType::Double) {
                type = EvaluatedType::Double;
            } else {
                throw AnalyzeException("Operand must be an integer or double");
            }

        } break;
    }

    expr.setType(type);
}

void CypherAnalyzer::analyze(SymbolExpression& expr) {
    VarDecl& var = _ctxt->getVariable(expr.symbol()._name);
    expr.setType(var.type());
}

void CypherAnalyzer::analyze(LiteralExpression& expr) {
    EvaluatedType type = EvaluatedType::Invalid;
    const auto& literal = expr.literal();

    switch (literal.type()) {
        case Literal::type<std::monostate>(): {
        } break;
        case Literal::type<bool>(): {
            type = EvaluatedType::Bool;
        } break;
        case Literal::type<int64_t>(): {
            type = EvaluatedType::Integer;
        } break;
        case Literal::type<double>(): {
            type = EvaluatedType::Double;
        } break;
        case Literal::type<std::string_view>(): {
            type = EvaluatedType::String;
        } break;
        case Literal::type<char>(): {
            type = EvaluatedType::Char;
        } break;
        case Literal::type<MapLiteral*>(): {
            type = EvaluatedType::Map;
        } break;
    }

    expr.setType(type);
}

void CypherAnalyzer::analyze(ParameterExpression& expr) {
    throw AnalyzeException("Parameters not supported");
}

void CypherAnalyzer::analyze(PropertyExpression& expr) {
    const auto& qualifiedName = expr.name();

    if (qualifiedName.size() != 2) {
        throw AnalyzeException("Only length 2 property expressions are supported");
    }

    std::string_view varName = qualifiedName.get(0);
    std::string_view propName = qualifiedName.get(1);

    const VarDecl& var = _ctxt->getVariable(varName);

    if (var.type() != EvaluatedType::NodePattern && var.type() != EvaluatedType::EdgePattern) {
        throw AnalyzeException(fmt::format("Variable '{}' is not a node or edge", varName));
    }

    expr.setDecl(&var);

    const std::optional<PropertyType> propType = _graphMetadata.propTypes().get(propName);

    if (!propType) {
        throw AnalyzeException(fmt::format("Property '{}' not found", propName));
    }

    EvaluatedType type = EvaluatedType::Invalid;

    switch (propType->_valueType) {
        case ValueType::UInt64:
        case ValueType::Int64: {
            type = EvaluatedType::Integer;
        } break;
        case ValueType::Bool: {
            type = EvaluatedType::Bool;
        } break;
        case ValueType::Double: {
            type = EvaluatedType::Double;
        } break;
        case ValueType::String: {
            type = EvaluatedType::String;
        } break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw AnalyzeException("Invalid property type");
    }

    expr.setType(type);
}

void CypherAnalyzer::analyze(StringExpression& expr) {
    auto& lhs = expr.left();
    auto& rhs = expr.right();

    analyze(lhs);
    analyze(rhs);

    if (lhs.type() != EvaluatedType::String || rhs.type() != EvaluatedType::String) {
        throw AnalyzeException("String expressions operands must be strings");
    }

    expr.setType(EvaluatedType::Bool);
}

void CypherAnalyzer::analyze(NodeLabelExpression& expr) {
    const auto& labelMap = _graphMetadata.labels();
    expr.setType(EvaluatedType::Bool);

    const auto& decl = _ctxt->getVariable(expr.symbol()._name);

    if (decl.type() != EvaluatedType::NodePattern) {
        throw AnalyzeException(fmt::format("Variable '{}' is not a node", decl.name()));
    }

    expr.setDecl(&decl);

    LabelSet& labelset = expr.labels();

    for (const auto& label : expr.labelNames()) {
        const std::optional<LabelID> labelID = labelMap.get(label);
        if (!labelID) {
            throw AnalyzeException(fmt::format("Unknown label: {}", label));
        }

        labelset.set(labelID.value());
    }
}

void CypherAnalyzer::analyze(PathExpression& expr) {
    throw AnalyzeException("Path expressions not supported");
}

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) {
    const auto* location = _ast->getLocation((uintptr_t)obj);
    std::string errorMsg;

    CypherError err {_ast->query()};
    err.setTitle("Query analysis error");
    err.setErrorMsg(msg);

    if (location) {
        err.setLocation(*location);
    }

    err.generate(errorMsg);

    throw AnalyzeException(std::move(errorMsg));
}
