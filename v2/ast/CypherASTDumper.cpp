#include "CypherASTDumper.h"

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "ASTException.h"
#include "attribution/ASTNodeDataStructs.h"
#include "expressions/AtomExpression.h"
#include "expressions/BinaryExpression.h"
#include "expressions/NodeLabelExpression.h"
#include "expressions/PathExpression.h"
#include "expressions/PropertyExpression.h"
#include "expressions/StringExpression.h"
#include "expressions/UnaryExpression.h"
#include "types/Literal.h"
#include "types/Projection.h"
#include "types/SinglePartQuery.h"
#include "statements/StatementContainer.h"
#include "statements/Match.h"
#include "statements/Return.h"

using namespace db;

std::string sanitizeString(std::string_view str) {
    std::string result = "_";
    std::transform(str.begin(), str.end(), std::back_inserter(result), [](char c) {
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            return c;
        }

        return '_';
    });

    return result;
}

CypherASTDumper::CypherASTDumper(const CypherAST& ast)
    : _ast(ast) {
}

void CypherASTDumper::dump(std::ostream& output) {
    _o = std::ostream_iterator<char>(output);

    fmt::format_to(_o, "---\n");
    fmt::format_to(_o, "config:\n");
    fmt::format_to(_o, "  layout: elk\n");

    fmt::format_to(_o, "---\n");
    fmt::format_to(_o, "erDiagram\n");

    fmt::format_to(_o, "    script {{ }} \n");

    for (const auto& query : _ast.queries()) {
        if (const auto* q = dynamic_cast<const SinglePartQuery*>(query.get())) {
            dump(*q);
            continue;
        }

        throw ASTException("Only single part queries are supported");
    }
}

void CypherASTDumper::dump(const SinglePartQuery& query) {
    fmt::format_to(_o, "    script ||--o{{ _{} : _\n", fmt::ptr(&query));
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&query));
    fmt::format_to(_o, "        ASTType SinglePartQuery\n");
    fmt::format_to(_o, "    }}\n");

    for (const auto& st : query.getStatements()) {
        if (const auto* matchSt = dynamic_cast<const Match*>(st)) {
            fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                           fmt::ptr(&query),
                           fmt::ptr(matchSt));

            dump(*matchSt);
            continue;
        }

        if (const auto* retSt = dynamic_cast<const Return*>(st)) {
            fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                           fmt::ptr(&query),
                           fmt::ptr(retSt));

            dump(*retSt);
            continue;
        }

        throw ASTException("Unknown statement type");
    }
}

void CypherASTDumper::dump(const Match& match) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&match));

    if (match.isOptional()) {
        fmt::format_to(_o, "        ASTType OPTIONAL_MATCH\n");
    } else {
        fmt::format_to(_o, "        ASTType MATCH\n");
    }

    fmt::format_to(_o, "    }}\n");

    const auto& pattern = match.getPattern();
    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&match),
                   fmt::ptr(&pattern));

    dump(pattern);

    if (match.hasLimit()) {
        const auto& lim = match.getLimit();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&match),
                       fmt::ptr(&lim));
        dump(lim);
    }

    if (match.hasSkip()) {
        const auto& skip = match.getSkip();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&match),
                       fmt::ptr(&skip));
        dump(skip);
    }
}

void CypherASTDumper::dump(const Limit& lim) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&lim));
    fmt::format_to(_o, "        ASTType LIMIT\n");
    fmt::format_to(_o, "    }}\n");

    const auto& expr = lim.getExpression();

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&lim),
                   fmt::ptr(&expr));

    dump(expr);
}

void CypherASTDumper::dump(const Skip& skip) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&skip));
    fmt::format_to(_o, "        ASTType SKIP\n");
    fmt::format_to(_o, "    }}\n");

    const auto& expr = skip.getExpression();

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&skip),
                   fmt::ptr(&expr));

    dump(expr);
}

void CypherASTDumper::dump(const Return& ret) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&ret));
    fmt::format_to(_o, "        ASTType RETURN\n");
    fmt::format_to(_o, "    }}\n");

    const auto& projection = ret.getProjection();

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&ret),
                   fmt::ptr(&projection));

    dump(projection);
}

void CypherASTDumper::dump(const Projection& projection) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&projection));
    fmt::format_to(_o, "        ASTType Projection\n");

    if (projection.isDistinct()) {
        fmt::format_to(_o, "        Distinct true\n");
    }

    if (projection.hasLimit()) {
        const auto& limit = projection.limit();
        fmt::format_to(_o, "        Limit _{}\n", fmt::ptr(&limit));
    }

    if (projection.hasSkip()) {
        const auto& skip = projection.skip();
        fmt::format_to(_o, "        Skip _{}\n", fmt::ptr(&skip));
    }


    if (projection.isAll()) {
        fmt::format_to(_o, "        ProjectAll true\n");
    } else {

        const auto& items = projection.items();
        for (const auto& item : items) {
            fmt::format_to(_o, "        Item _{}\n", fmt::ptr(item));
        }
    }

    fmt::format_to(_o, "    }}\n");

    if (projection.hasLimit()) {
        const auto& limit = projection.limit();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&projection),
                       fmt::ptr(&limit));
        dump(limit);
    }

    if (projection.hasSkip()) {
        const auto& skip = projection.skip();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&projection),
                       fmt::ptr(&skip));
        dump(skip);
    }

    if (projection.isAll()) {
        return;
    }

    const auto& items = projection.items();
    for (const auto& item : items) {
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&projection),
                       fmt::ptr(item));
        dump(*item);
    }
}

void CypherASTDumper::dump(const Pattern& pattern) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&pattern));
    fmt::format_to(_o, "        ASTType Pattern\n");
    fmt::format_to(_o, "    }}\n");

    for (const auto& part : pattern.elements()) {
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&pattern),
                       fmt::ptr(part));
        dump(*part);
    }

    if (pattern.hasWhere()) {
        const auto& where = pattern.getWhere();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&pattern),
                       fmt::ptr(&where));
        dump(where);
    }
}

void CypherASTDumper::dump(const PatternElement& elem) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&elem));
    fmt::format_to(_o, "        ASTType PatternElement\n");
    fmt::format_to(_o, "    }}\n");

    for (const auto& entity : elem.getEntities()) {
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&elem),
                       fmt::ptr(entity));
        if (auto* node = dynamic_cast<NodePattern*>(entity)) {
            dump(*node);
        }

        else if (auto* edge = dynamic_cast<EdgePattern*>(entity)) {
            dump(*edge);
        }
    }
}

void CypherASTDumper::dump(const WhereClause& where) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&where));
    fmt::format_to(_o, "        ASTType WHERE\n");
    fmt::format_to(_o, "    }}\n");

    const auto& expr = where.getExpression();
    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&where),
                   fmt::ptr(&expr));
    dump(expr);
}

void CypherASTDumper::dump(const NodePattern& node) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&node));
    fmt::format_to(_o, "        ASTType NodePattern\n");

    if (node.hasSymbol()) {
        fmt::format_to(_o, "        Symbol {}\n", node.symbol()._name);
    }

    if (node.hasLabels()) {
        for (const auto& type : node.labels()) {
            fmt::format_to(_o, "        Label {}\n", type);
        }
    }

    if (node.hasProperties()) {
        const auto& properties = node.properties();
        for (const auto& [k, v] : properties) {
            fmt::format_to(_o, "        prop_{} _{}\n", sanitizeString(k), fmt::ptr(v));
        }
    }

    fmt::format_to(_o, "    }}\n");

    if (!node.analyzed()) {
        fmt::format_to(_o, "    _{} ||--o{{ _NOT_ANALYZED : _\n",
                       fmt::ptr(&node));
    } else {
        const VariableDecl* var = node.data().as<NodePatternData>()._decl;
        if (var) {
            fmt::format_to(_o, "    _{} ||--o{{ VAR_{} : _\n",
                           fmt::ptr(&node),
                           var->id());
            dump(*var);
        }
    }
}

void CypherASTDumper::dump(const EdgePattern& edge) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&edge));
    fmt::format_to(_o, "        ASTType EdgePattern\n");

    if (edge.hasSymbol()) {
        const auto& symbol = edge.symbol();
        fmt::format_to(_o, "        Name {}\n", symbol._name);
    }

    if (edge.hasTypes()) {
        for (const auto& type : edge.types()) {
            fmt::format_to(_o, "        EdgeType {}\n", type);
        }
    }

    if (edge.hasProperties()) {
        const auto& properties = edge.properties();
        for (const auto& [k, v] : properties) {
            fmt::format_to(_o, "        prop_{} _{}\n", sanitizeString(k), fmt::ptr(v));
        }
    }

    fmt::format_to(_o, "    }}\n");

    if (!edge.analyzed()) {
        fmt::format_to(_o, "    _{} ||--o{{ _NOT_ANALYZED : _\n",
                       fmt::ptr(&edge));
    } else {
        const VariableDecl* var = edge.data().as<EdgePatternData>()._decl;
        if (var) {
            fmt::format_to(_o, "    _{} ||--o{{ VAR_{} : _\n",
                           fmt::ptr(&edge),
                           var->id());
            dump(*var);
        }
    }
}

void CypherASTDumper::dump(const MapLiteral& map) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&map));
    fmt::format_to(_o, "        ASTType MapLiteral\n");

    for (const auto& [k, v] : map) {
        fmt::format_to(_o, "        prop_{} _{}\n", sanitizeString(k), fmt::ptr(v));
    }

    fmt::format_to(_o, "    }}\n");


    for (const auto& [k, v] : map) {
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&map),
                       fmt::ptr(v));
        dump(*v);
    }
}

void CypherASTDumper::dump(const Expression& expr) {
    if (const auto* e = dynamic_cast<const BinaryExpression*>(&expr)) {
        dump(*e);
    } else if (const auto* e = dynamic_cast<const UnaryExpression*>(&expr)) {
        dump(*e);
    } else if (const auto* e = dynamic_cast<const AtomExpression*>(&expr)) {
        dump(*e);
    } else if (const auto* e = dynamic_cast<const PathExpression*>(&expr)) {
        dump(*e);
    } else if (const auto* e = dynamic_cast<const NodeLabelExpression*>(&expr)) {
        dump(*e);
    } else if (const auto* e = dynamic_cast<const StringExpression*>(&expr)) {
        dump(*e);
    } else if (const auto* e = dynamic_cast<const PropertyExpression*>(&expr)) {
        dump(*e);
    } else {
        throw ASTException("Unknown expression type");
    }
}

void CypherASTDumper::dump(const BinaryExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType BinaryExpression\n");

    if (expr.id().valid()) {
        const AnalysisData& data = _ast.getAnalysisData(expr.id());
        fmt::format_to(_o, "        ValueType {}\n", EvaluatedTypeName::value(data.type()));
    } else {
        fmt::format_to(_o, "        ValueType NOT_EVALUATED\n");
    }

    switch (expr.getBinaryOperator()) {
        case BinaryOperator::And:
            fmt::format_to(_o, "        Operator AND\n");
            break;
        case BinaryOperator::Xor:
            fmt::format_to(_o, "        Operator XOR\n");
            break;
        case BinaryOperator::Or:
            fmt::format_to(_o, "        Operator OR\n");
            break;
        case BinaryOperator::NotEqual:
            fmt::format_to(_o, "        Operator NOT_EQUAL\n");
            break;
        case BinaryOperator::Equal:
            fmt::format_to(_o, "        Operator EQUAL\n");
            break;
        case BinaryOperator::LessThan:
            fmt::format_to(_o, "        Operator LESS_THAN\n");
            break;
        case BinaryOperator::GreaterThan:
            fmt::format_to(_o, "        Operator GREATER_THAN\n");
            break;
        case BinaryOperator::LessThanOrEqual:
            fmt::format_to(_o, "        Operator LESS_THAN_OR_EQUAL\n");
            break;
        case BinaryOperator::GreaterThanOrEqual:
            fmt::format_to(_o, "        Operator GREATER_THAN_OR_EQUAL\n");
            break;
        case BinaryOperator::Add:
            fmt::format_to(_o, "        Operator ADD\n");
            break;
        case BinaryOperator::Sub:
            fmt::format_to(_o, "        Operator SUB\n");
            break;
        case BinaryOperator::Mult:
            fmt::format_to(_o, "        Operator MULT\n");
            break;
        case BinaryOperator::Div:
            fmt::format_to(_o, "        Operator DIV\n");
            break;
        case BinaryOperator::Mod:
            fmt::format_to(_o, "        Operator MOD\n");
            break;
        case BinaryOperator::Pow:
            fmt::format_to(_o, "        Operator POW\n");
            break;
        case BinaryOperator::In:
            fmt::format_to(_o, "        Operator IN\n");
            break;
    }

    fmt::format_to(_o, "    }}\n");

    const auto& left = expr.left();
    const auto& right = expr.right();

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&expr),
                   fmt::ptr(&left));

    dump(left);

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&expr),
                   fmt::ptr(&right));

    dump(right);
}

void CypherASTDumper::dump(const UnaryExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType UnaryExpression\n");

    if (expr.id().valid()) {
        const AnalysisData& data = _ast.getAnalysisData(expr.id());
        fmt::format_to(_o, "        ValueType {}\n", EvaluatedTypeName::value(data.type()));
    } else {
        fmt::format_to(_o, "        ValueType NOT_EVALUATED\n");
    }

    switch (expr.getUnaryOperator()) {
        case UnaryOperator::Not:
            fmt::format_to(_o, "        Operator NOT\n");
            break;
        case UnaryOperator::Minus:
            fmt::format_to(_o, "        Operator MINUS\n");
            break;
        case UnaryOperator::Plus:
            fmt::format_to(_o, "        Operator PLUS\n");
            break;
    }

    fmt::format_to(_o, "    }}\n");

    const auto& right = expr.right();

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&expr),
                   fmt::ptr(&right));

    dump(right);
}

void CypherASTDumper::dump(const AtomExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType AtomExpression\n");

    if (expr.id().valid()) {
        const AnalysisData& data = _ast.getAnalysisData(expr.id());
        fmt::format_to(_o, "        ValueType {}\n", EvaluatedTypeName::value(data.type()));
    } else {
        fmt::format_to(_o, "        ValueType NOT_EVALUATED\n");
    }

    const auto& value = expr.value();

    const auto visitor = [this](const auto& v) {
        if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Symbol>) {
            fmt::format_to(_o, "        Symbol {}\n", v._name);
        } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Literal>) {
            const auto& l = v.value();
            if (const auto* v = std::get_if<std::monostate>(&l)) {
                fmt::format_to(_o, "        NullLiteral _\n");
            } else if (const auto* v = std::get_if<bool>(&l)) {
                fmt::format_to(_o, "        BoolLiteral {}\n", *v);
            } else if (const auto* v = std::get_if<int64_t>(&l)) {
                fmt::format_to(_o, "        IntLiteral _{}\n", *v);
            } else if (const auto* v = std::get_if<double>(&l)) {
                fmt::format_to(_o, "        DoubleLiteral _{}\n", *v);
            } else if (const auto* v = std::get_if<std::string_view>(&l)) {
                fmt::format_to(_o, "        StringLiteral {}\n", sanitizeString(*v));
            } else if (const auto* v = std::get_if<char>(&l)) {
                fmt::format_to(_o, "        CharLiteral _{}\n", *v);
            } else if (const auto* v = std::get_if<MapLiteral*>(&l)) {
                fmt::format_to(_o, "        MapLiteral _{}\n", fmt::ptr(*v));
            }

        } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Parameter>) {
            fmt::format_to(_o, "        Parameter {}\n", sanitizeString(v._name));
        }
    };


    std::visit(visitor, value);

    fmt::format_to(_o, "    }}\n");

    const auto& data = _ast.getAnalysisData(expr.id());
    if (data.is<SymbolData>()) {
        const auto& decl = data.as<SymbolData>()._decl;
        fmt::format_to(_o, "    _{} ||--o{{ VAR_{} : _\n",
                       fmt::ptr(&expr),
                       decl.id());
    }
}

void CypherASTDumper::dump(const PathExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType PathExpression\n");
    fmt::format_to(_o, "    }}\n");
}

void CypherASTDumper::dump(const NodeLabelExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType NodeLabelExpression\n");
    fmt::format_to(_o, "        ValueType {}\n", EvaluatedTypeName::value(EvaluatedType::Bool));

    const auto& label = expr.labels();
    for (const auto& l : label) {
        fmt::format_to(_o, "        Label {}\n", l);
    }

    fmt::format_to(_o, "    }}\n");

    if (expr.id().valid()) {
        const auto& data = _ast.getAnalysisData(expr.id()).as<NodeLabelExpressionData>();
        fmt::format_to(_o, "    _{} ||--o{{ VAR_{} : _\n",
                       fmt::ptr(&expr),
                       data._var.id());
    }
}

void CypherASTDumper::dump(const StringExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType StringExpression\n");
    fmt::format_to(_o, "        ValueType {}\n", EvaluatedTypeName::value(EvaluatedType::Bool));

    switch (expr.getStringOperator()) {
        case StringOperator::StartsWith:
            fmt::format_to(_o, "        Operator STARTS_WITH\n");
            break;
        case StringOperator::EndsWith:
            fmt::format_to(_o, "        Operator ENDS_WITH\n");
            break;
        case StringOperator::Contains:
            fmt::format_to(_o, "        Operator CONTAINS\n");
            break;
    }

    fmt::format_to(_o, "    }}\n");

    const auto& left = expr.left();
    const auto& right = expr.right();

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&expr),
                   fmt::ptr(&right));

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&expr),
                   fmt::ptr(&left));

    dump(left);
    dump(right);
}

void CypherASTDumper::dump(const PropertyExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType PropertyExpression\n");

    if (expr.id().valid()) {
        const AnalysisData& data = _ast.getAnalysisData(expr.id());
        fmt::format_to(_o, "        ValueType {}\n", EvaluatedTypeName::value(data.type()));
    } else {
        fmt::format_to(_o, "        ValueType NOT_EVALUATED\n");
    }

    fmt::format_to(_o, "        QualifiedName ");

    size_t i = 0;
    for (const auto& name : expr.name()) {
        if (i != 0) {
            fmt::format_to(_o, "_");
        }

        fmt::format_to(_o, "{}", name);
        i++;
    }

    fmt::format_to(_o, "\n    }}\n");

    if (expr.id().valid()) {
        const auto& data = _ast.getAnalysisData(expr.id()).as<PropertyExpressionData>();
        fmt::format_to(_o, "    _{} ||--o{{ VAR_{} : _\n",
                       fmt::ptr(&expr),
                       data._var.id());
    }
}

void CypherASTDumper::dump(const ConstVariableDecl& decl) {
    if (_dumpedVariables.contains(decl.id())) {
        return;
    }

    _dumpedVariables.insert(decl.id());

    fmt::format_to(_o, "    VAR_{} {{\n", decl.id());
    fmt::format_to(_o, "        ValueType {}\n", EvaluatedTypeName::value(decl.type()));

    const auto& name = decl.name();

    if (!name.empty()) {
        fmt::format_to(_o, "        Name {}\n", name);
    }

    fmt::format_to(_o, "    }}\n");
}
