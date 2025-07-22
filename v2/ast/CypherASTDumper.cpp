#include "CypherASTDumper.h"

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "ASTException.h"
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
    : _ast(ast)
{
}

void CypherASTDumper::dump(std::ostream& output) {
    _o = std::ostream_iterator<char>(output);

    fmt::format_to(_o, "---\n");
    fmt::format_to(_o, "config:\n");
    fmt::format_to(_o, "  layout: dagre\n");
    fmt::format_to(_o, "---\n");
    fmt::format_to(_o, "erDiagram\n");

    fmt::format_to(_o, "    script {{ }} \n");

    for (const auto& query : _ast.queries()) {
        if (const auto* q = dynamic_cast<const SinglePartQuery*>(query.get())) {
            dumpQuery(*q);
            continue;
        }

        throw ASTException("Only single part queries are supported");
    }
}

void CypherASTDumper::dumpQuery(const SinglePartQuery& query) {
    fmt::format_to(_o, "    script ||--o{{ _{} : _\n", fmt::ptr(&query));
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&query));
    fmt::format_to(_o, "        ASTType SinglePartQuery\n");
    fmt::format_to(_o, "    }}\n");

    for (const auto& st : query.getStatements()) {
        if (const auto* matchSt = dynamic_cast<const Match*>(st)) {
            fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                           fmt::ptr(&query),
                           fmt::ptr(matchSt));

            dumpMatch(*matchSt);
            continue;
        }

        if (const auto* retSt = dynamic_cast<const Return*>(st)) {
            fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                           fmt::ptr(&query),
                           fmt::ptr(retSt));

            dumpReturn(*retSt);
            continue;
        }

        throw ASTException("Unknown statement type");
    }
}

void CypherASTDumper::dumpMatch(const Match& match) {
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

    dumpPattern(pattern);

    if (match.hasLimit()) {
        const auto& lim = match.getLimit();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&match),
                       fmt::ptr(&lim));
        dumpLimit(lim);
    }

    if (match.hasSkip()) {
        const auto& skip = match.getSkip();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&match),
                       fmt::ptr(&skip));
        dumpSkip(skip);
    }
}

void CypherASTDumper::dumpLimit(const Limit& lim) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&lim));
    fmt::format_to(_o, "        ASTType LIMIT\n");
    fmt::format_to(_o, "    }}\n");
}

void CypherASTDumper::dumpSkip(const Skip& skip) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&skip));
    fmt::format_to(_o, "        ASTType SKIP\n");
    fmt::format_to(_o, "    }}\n");
}

void CypherASTDumper::dumpReturn(const Return& ret) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&ret));
    fmt::format_to(_o, "        ASTType RETURN\n");
    fmt::format_to(_o, "    }}\n");

    const auto& projection = ret.getProjection();

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&ret),
                   fmt::ptr(&projection));

    dumpProjection(projection);
}

void CypherASTDumper::dumpProjection(const Projection& projection) {
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
        dumpLimit(limit);
    }

    if (projection.hasSkip()) {
        const auto& skip = projection.skip();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&projection),
                       fmt::ptr(&skip));
        dumpSkip(skip);
    }

    if (projection.isAll()) {
        return;
    }

    const auto& items = projection.items();
    for (const auto& item : items) {
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&projection),
                       fmt::ptr(item));
        dumpExpression(*item);
    }
}

void CypherASTDumper::dumpPattern(const Pattern& pattern) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&pattern));
    fmt::format_to(_o, "        ASTType Pattern\n");
    fmt::format_to(_o, "    }}\n");

    for (const auto& part : pattern.elements()) {
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&pattern),
                       fmt::ptr(part));
        dumpPatternPart(*part);
    }

    if (pattern.hasWhere()) {
        const auto& where = pattern.getWhere();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&pattern),
                       fmt::ptr(&where));
        dumpWhere(where);
    }
}

void CypherASTDumper::dumpPatternPart(const PatternElement& part) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&part));
    fmt::format_to(_o, "        ASTType PatternPart\n");
    fmt::format_to(_o, "    }}\n");

    for (const auto& entity : part.getEntities()) {
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&part),
                       fmt::ptr(entity));
        if (auto* node = dynamic_cast<NodePattern*>(entity)) {
            dumpNode(*node);
        }

        else if (auto* edge = dynamic_cast<EdgePattern*>(entity)) {
            dumpEdge(*edge);
        }
    }
}

void CypherASTDumper::dumpWhere(const WhereClause& where) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&where));
    fmt::format_to(_o, "        ASTType WHERE\n");
    fmt::format_to(_o, "    }}\n");

    const auto& expr = where.getExpression();
    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&where),
                   fmt::ptr(&expr));
    dumpExpression(expr);
}

void CypherASTDumper::dumpNode(const NodePattern& node) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&node));
    fmt::format_to(_o, "        ASTType NodePattern\n");

    if (node.hasSymbol()) {
        fmt::format_to(_o, "        Symbol {}\n", node.symbol()._name);
    }

    fmt::format_to(_o, "    }}\n");

    if (node.hasSymbol()) {
        const auto& symbol = node.symbol();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&node),
                       fmt::ptr(&symbol));
        dumpSymbol(symbol);
    }

    if (node.hasProperties()) {
        const auto& properties = node.properties();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&node),
                       fmt::ptr(&properties));
        dumpMapLiteral(properties);
    }

    if (node.hasLabels()) {
        const auto& labels = node.labels();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&node),
                       fmt::ptr(&labels));
        dumpTypes(labels);
    }
}

void CypherASTDumper::dumpEdge(const EdgePattern& edge) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&edge));
    fmt::format_to(_o, "        ASTType EdgePattern\n");

    if (edge.hasSymbol()) {
        const auto& symbol = edge.symbol();
        fmt::format_to(_o, "        Name {}\n", symbol._name);
    }

    fmt::format_to(_o, "    }}\n");

    if (edge.hasSymbol()) {
        const auto& symbol = edge.symbol();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&edge),
                       fmt::ptr(&symbol));
        dumpSymbol(symbol);
    }
    if (edge.hasProperties()) {
        const auto& properties = edge.properties();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&edge),
                       fmt::ptr(&properties));
        dumpMapLiteral(properties);
    }
    if (edge.hasTypes()) {
        const auto& labels = edge.types();
        fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                       fmt::ptr(&edge),
                       fmt::ptr(&labels));
        dumpTypes(labels);
    }
}

void CypherASTDumper::dumpSymbol(const Symbol& symbol) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&symbol));
    fmt::format_to(_o, "        ASTType Symbol\n");
    fmt::format_to(_o, "        Name {}\n", symbol._name);
    fmt::format_to(_o, "    }}\n");
}

void CypherASTDumper::dumpMapLiteral(const MapLiteral& map) {
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
        dumpExpression(*v);
    }
}

void CypherASTDumper::dumpTypes(const std::vector<std::string_view>& types) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&types));
    fmt::format_to(_o, "        ASTType Types\n");

    for (const auto& type : types) {
        fmt::format_to(_o, "        Type {}\n", type);
    }

    fmt::format_to(_o, "    }}\n");
}


void CypherASTDumper::dumpExpression(const Expression& expr) {
    if (const auto* e = dynamic_cast<const BinaryExpression*>(&expr)) {
        dumpBinaryExpression(*e);
        return;
    }

    if (const auto* e = dynamic_cast<const UnaryExpression*>(&expr)) {
        dumpUnaryExpression(*e);
        return;
    }

    if (const auto* e = dynamic_cast<const AtomExpression*>(&expr)) {
        dumpAtomExpression(*e);
        return;
    }

    if (const auto* e = dynamic_cast<const PathExpression*>(&expr)) {
        dumpPathExpression(*e);
        return;
    }

    if (const auto* e = dynamic_cast<const NodeLabelExpression*>(&expr)) {
        dumpNodeLabelExpression(*e);
        return;
    }

    if (const auto* e = dynamic_cast<const StringExpression*>(&expr)) {
        dumpStringExpression(*e);
        return;
    }

    if (const auto* e = dynamic_cast<const PropertyExpression*>(&expr)) {
        dumpPropertyExpression(*e);
        return;
    }

    throw ASTException("Unknown expression type");
}

void CypherASTDumper::dumpBinaryExpression(const BinaryExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType BinaryExpression\n");

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

    dumpExpression(left);

    fmt::format_to(_o, "    _{} ||--o{{ _{} : _\n",
                   fmt::ptr(&expr),
                   fmt::ptr(&right));

    dumpExpression(right);
}

void CypherASTDumper::dumpUnaryExpression(const UnaryExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType UnaryExpression\n");

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

    dumpExpression(right);
}

void CypherASTDumper::dumpAtomExpression(const AtomExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType AtomExpression\n");

    const auto& value = expr.value();

    const auto visitor = [this](const auto& v) {
        if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Symbol>) {
            fmt::format_to(_o, "        Symbol {}\n", v._name);
        } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Literal>) {
            const auto& l = v.value();
            if (std::holds_alternative<bool>(l)) {
                fmt::format_to(_o, "        BoolLiteral {}\n", std::get<bool>(l));
            } else if (std::holds_alternative<int64_t>(l)) {
                fmt::format_to(_o, "        IntLiteral _{}\n", std::get<int64_t>(l));
            } else if (std::holds_alternative<double>(l)) {
                fmt::format_to(_o, "        DoubleLiteral _{}\n", std::get<double>(l));
            } else if (std::holds_alternative<std::string_view>(l)) {
                fmt::format_to(_o, "        StringLiteral {}\n", sanitizeString(std::get<std::string_view>(l)));
            } else if (std::holds_alternative<char>(l)) {
                fmt::format_to(_o, "        CharLiteral _{}\n", std::get<char>(l));
            } else if (std::holds_alternative<MapLiteral*>(l)) {
                fmt::format_to(_o, "        MapLiteral _{}\n", fmt::ptr(std::get<MapLiteral*>(l)));
            } else if (std::holds_alternative<std::nullopt_t>(l)) {
                fmt::format_to(_o, "        NullLiteral _\n");
            }

        } else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Parameter>) {
            fmt::format_to(_o, "        Parameter {}\n", sanitizeString(v._name));
        }
    };


    std::visit(visitor, value);

    fmt::format_to(_o, "    }}\n");
}

void CypherASTDumper::dumpPathExpression(const PathExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType PathExpression\n");
    fmt::format_to(_o, "    }}\n");
}

void CypherASTDumper::dumpNodeLabelExpression(const NodeLabelExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType NodeLabelExpression\n");
    fmt::format_to(_o, "    }}\n");
}

void CypherASTDumper::dumpStringExpression(const StringExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType StringExpression\n");

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

    dumpExpression(left);
    dumpExpression(right);
}

void CypherASTDumper::dumpPropertyExpression(const PropertyExpression& expr) {
    fmt::format_to(_o, "    _{} {{\n", fmt::ptr(&expr));
    fmt::format_to(_o, "        ASTType PropertyExpression\n");
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
}
