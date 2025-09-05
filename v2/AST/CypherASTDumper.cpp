#include "CypherASTDumper.h"

#include "CypherAST.h"
#include "ASTException.h"
#include "decl/DeclContainer.h"
#include "expressions/All.h"
#include "types/Literal.h"
#include "types/Projection.h"
#include "types/SinglePartQuery.h"
#include "statements/StatementContainer.h"
#include "statements/Match.h"
#include "statements/Return.h"
#include "decl/VarDecl.h"

using namespace db::v2;

namespace {

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

}

CypherASTDumper::CypherASTDumper(const CypherAST& ast)
    : _ast(ast)
{
}

void CypherASTDumper::dump(std::ostream& out) {
    out << "---\n";
    out << "config:\n";
    out << "  layout: elk\n";

    out << "---\n";
    out << "erDiagram\n";

    out << "    script {{ }} \n";

    for (const auto& query : _ast.queries()) {
        if (const auto* q = dynamic_cast<const SinglePartQuery*>(query.get())) {
            dump(out, *q);
            continue;
        }

        throw ASTException("Only single part queries are supported");
    }
}

void CypherASTDumper::dump(std::ostream& out, const SinglePartQuery& query) {
    out << "    script ||--o{{ _" << std::hex << &query << " : _\n";
    out << "    _" << std::hex << &query << " {{\n";
    out << "        ASTType SinglePartQuery\n";
    out << "    }}\n";

    for (const auto& st : query.getStatements()) {
        if (const auto* matchSt = dynamic_cast<const Match*>(st)) {
            out << "    _" << std::hex << &query << " ||--o{{ _" << std::hex << matchSt << " : _\n";

            dump(out, *matchSt);
            continue;
        }

        if (const auto* retSt = dynamic_cast<const Return*>(st)) {
            out << "    _" << std::hex << &query << " ||--o{{ _" << std::hex << retSt << " : _\n";

            dump(out, *retSt);
            continue;
        }

        throw ASTException("Unknown statement type");
    }
}

void CypherASTDumper::dump(std::ostream& out, const Match& match) {
    out << "    _" << std::hex << &match << " {{\n";

    if (match.isOptional()) {
        out << "        ASTType OPTIONAL_MATCH\n";
    } else {
        out << "        ASTType MATCH\n";
    }

    out << "    }}\n";

    const auto& pattern = match.getPattern();
    out << "    _" << std::hex << &match << " ||--o{{ _" << std::hex << &pattern << " : _\n";

    dump(out, pattern);

    if (match.hasLimit()) {
        const auto& lim = match.getLimit();
        out << "    _" << std::hex << &match << " ||--o{{ _" << std::hex << &lim << " : _\n";
        dump(out, lim);
    }

    if (match.hasSkip()) {
        const auto& skip = match.getSkip();
        out << "    _" << std::hex << &match << " ||--o{{ _" << std::hex << &skip << " : _\n";
        dump(out, skip);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Limit& lim) {
    out << "    _" << std::hex << &lim << " {{\n";
    out << "        ASTType LIMIT\n";
    out << "    }}\n";

    const auto& expr = lim.getExpression();

    out << "    _" << std::hex << &lim << " ||--o{{ _" << std::hex << &expr << " : _\n";

    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const Skip& skip) {
    out << "    _" << std::hex << &skip << " {{\n";
    out << "        ASTType SKIP\n";
    out << "    }}\n";

    const auto& expr = skip.getExpression();

    out << "    _" << std::hex << &skip << " ||--o{{ _" << std::hex << &expr << " : _\n";

    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const Return& ret) {
    out << "    _" << std::hex << &ret << " {{\n";
    out << "        ASTType RETURN\n";
    out << "    }}\n";

    const auto& projection = ret.getProjection();

    out << "    _" << std::hex << &ret << " ||--o{{ _" << std::hex << &projection << " : _\n";

    dump(out, projection);
}

void CypherASTDumper::dump(std::ostream& out, const Projection& projection) {
    out << "    _" << std::hex << &projection << " {{\n";
    out << "        ASTType Projection\n";

    if (projection.isDistinct()) {
        out << "        Distinct true\n";
    }

    if (projection.hasLimit()) {
        const auto& limit = projection.limit();
        out << "        Limit _" << std::hex << &limit << "\n";
    }

    if (projection.hasSkip()) {
        const auto& skip = projection.skip();
        out << "        Skip _" << std::hex << &skip << "\n";
    }


    if (projection.isAll()) {
        out << "        ProjectAll true\n";
    } else {

        const auto& items = projection.items();
        for (const auto& item : items) {
            out << "        Item _" << std::hex << item << "\n";
        }
    }

    out << "    }}\n";

    if (projection.hasLimit()) {
        const auto& limit = projection.limit();
        out << "    _" << std::hex << &projection << " ||--o{{ _" << std::hex << &limit << " : _\n";
        dump(out, limit);
    }

    if (projection.hasSkip()) {
        const auto& skip = projection.skip();
        out << "    _" << std::hex << &projection << " ||--o{{ _" << std::hex << &skip << " : _\n";
        dump(out, skip);
    }

    if (projection.isAll()) {
        return;
    }

    const auto& items = projection.items();
    for (const auto& item : items) {
        out << "    _" << std::hex << &projection << " ||--o{{ _" << std::hex << item << " : _\n";
        dump(out, *item);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Pattern& pattern) {
    out << "    _" << std::hex << &pattern << " {{\n";
    out << "        ASTType Pattern\n";
    out << "    }}\n";

    for (const auto& part : pattern.elements()) {
        out << "    _" << std::hex << &pattern << " ||--o{{ _" << std::hex << part << " : _\n";
        dump(out, *part);
    }

    if (pattern.hasWhere()) {
        const auto& where = pattern.getWhere();
        out << "    _" << std::hex << &pattern << " ||--o{{ _" << std::hex << &where << " : _\n";
        dump(out, where);
    }
}

void CypherASTDumper::dump(std::ostream& out, const PatternElement& elem) {
    out << "    _" << std::hex << &elem << " {{\n";
    out << "        ASTType PatternElement\n";
    out << "    }}\n";

    for (const auto& entity : elem.getEntities()) {
        out << "    _" << std::hex << &elem << " ||--o{{ _" << std::hex << entity << " : _\n";
        if (auto* node = dynamic_cast<NodePattern*>(entity)) {
            dump(out, *node);
        }

        else if (auto* edge = dynamic_cast<EdgePattern*>(entity)) {
            dump(out, *edge);
        }
    }
}

void CypherASTDumper::dump(std::ostream& out, const WhereClause& where) {
    out << "    _" << std::hex << &where << " {{\n";
    out << "        ASTType WHERE\n";
    out << "    }}\n";

    const auto& expr = where.getExpression();
    out << "    _" << std::hex << &where << " ||--o{{ _" << std::hex << &expr << " : _\n";
    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const NodePattern& node) {
    out << "    _" << std::hex << &node << " {{\n";
    out << "        ASTType NodePattern\n";

    if (node.hasSymbol()) {
        out << "        Symbol " << node.symbol()._name << "\n";
    }

    if (node.hasLabels()) {
        for (const auto& type : node.labels()) {
            out << "        Label " << type << "\n";
        }
    }

    if (node.hasProperties()) {
        const auto& properties = node.properties();
        for (const auto& [k, v] : properties) {
            out << "        prop_" << sanitizeString(k) << " _" << std::hex << v << "\n";
        }
    }

    out << "    }}\n";

    if (!node.hasDecl()) {
        return;
    }

    const auto& decl = node.decl();
    out << "    _" << std::hex << &node << " ||--o{{ VAR_" << decl.id() << " : _\n";
    dump(out, decl);
}

void CypherASTDumper::dump(std::ostream& out, const EdgePattern& edge) {
    out << "    _" << std::hex << &edge << " {{\n";
    out << "        ASTType EdgePattern\n";

    if (edge.hasSymbol()) {
        const auto& symbol = edge.symbol();
        out << "        Name " << symbol._name << "\n";
    }

    if (edge.hasTypes()) {
        for (const auto& type : edge.types()) {
            out << "        EdgeType " << type << "\n";
        }
    }

    if (edge.hasProperties()) {
        const auto& properties = edge.properties();
        for (const auto& [k, v] : properties) {
            out << "        prop_" << sanitizeString(k) << " _" << std::hex << v << "\n";
        }
    }

    out << "    }}\n";

    if (!edge.hasDecl()) {
        return;
    }

    const auto& decl = edge.decl();
    out << "    _" << std::hex << &edge << " ||--o{{ VAR_" << decl.id() << " : _\n";
    dump(out, decl);
}

void CypherASTDumper::dump(std::ostream& out, const MapLiteral& map) {
    out << "    _" << std::hex << &map << " {{\n";
    out << "        ASTType MapLiteral\n";

    for (const auto& [k, v] : map) {
        out << "        prop_" << sanitizeString(k) << " _" << std::hex << v << "\n";
    }

    out << "    }}\n";


    for (const auto& [k, v] : map) {
        out << "    _" << std::hex << &map << " ||--o{{ _" << std::hex << v << " : _\n";
        dump(out, *v);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Expression& expr) {
    switch (expr.kind()) {
        case ExpressionType::Binary:
            dump(out, *expr.as<BinaryExpression>());
            break;
        case ExpressionType::Unary:
            dump(out, *expr.as<UnaryExpression>());
            break;
        case ExpressionType::String:
            dump(out, *expr.as<StringExpression>());
            break;
        case ExpressionType::NodeLabel:
            dump(out, *expr.as<NodeLabelExpression>());
            break;
        case ExpressionType::Property:
            dump(out, *expr.as<PropertyExpression>());
            break;
        case ExpressionType::Path:
            dump(out, *expr.as<PathExpression>());
            break;
        case ExpressionType::Symbol:
            dump(out, *expr.as<SymbolExpression>());
            break;
        case ExpressionType::Literal:
            dump(out, *expr.as<LiteralExpression>());
            break;
        case ExpressionType::Parameter:
            dump(out, *expr.as<ParameterExpression>());
            break;

        default:
            throw ASTException("Unknown expression type");
            break;
    }
}

void CypherASTDumper::dump(std::ostream& out, const BinaryExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType BinaryExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr.type()) << "\n";

    switch (expr.getBinaryOperator()) {
        case BinaryOperator::And:
            out << "        Operator AND\n";
            break;
        case BinaryOperator::Xor:
            out << "        Operator XOR\n";
            break;
        case BinaryOperator::Or:
            out << "        Operator OR\n";
            break;
        case BinaryOperator::NotEqual:
            out << "        Operator NOT_EQUAL\n";
            break;
        case BinaryOperator::Equal:
            out << "        Operator EQUAL\n";
            break;
        case BinaryOperator::LessThan:
            out << "        Operator LESS_THAN\n";
            break;
        case BinaryOperator::GreaterThan:
            out << "        Operator GREATER_THAN\n";
            break;
        case BinaryOperator::LessThanOrEqual:
            out << "        Operator LESS_THAN_OR_EQUAL\n";
            break;
        case BinaryOperator::GreaterThanOrEqual:
            out << "        Operator GREATER_THAN_OR_EQUAL\n";
            break;
        case BinaryOperator::Add:
            out << "        Operator ADD\n";
            break;
        case BinaryOperator::Sub:
            out << "        Operator SUB\n";
            break;
        case BinaryOperator::Mult:
            out << "        Operator MULT\n";
            break;
        case BinaryOperator::Div:
            out << "        Operator DIV\n";
            break;
        case BinaryOperator::Mod:
            out << "        Operator MOD\n";
            break;
        case BinaryOperator::Pow:
            out << "        Operator POW\n";
            break;
        case BinaryOperator::In:
            out << "        Operator IN\n";
            break;
        default:
            throw ASTException("Unknown binary operator");
            break;
    }

    out << "    }}\n";

    const auto& left = expr.left();
    const auto& right = expr.right();

    out << "    _" << std::hex << &expr << " ||--o{{ _" << std::hex << &left << " : _\n";

    dump(out, left);

    out << "    _" << std::hex << &expr << " ||--o{{ _" << std::hex << &right << " : _\n";

    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const UnaryExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType UnaryExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr.type()) << "\n";

    switch (expr.getUnaryOperator()) {
        case UnaryOperator::Not:
            out << "        Operator NOT\n";
            break;
        case UnaryOperator::Minus:
            out << "        Operator MINUS\n";
            break;
        case UnaryOperator::Plus:
            out << "        Operator PLUS\n";
            break;
        default:
            throw ASTException("Unknown unary operator");
            break;
    }

    out << "    }}\n";

    const auto& right = expr.right();

    out << "    _" << std::hex << &expr << " ||--o{{ _" << std::hex << &right << " : _\n";

    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const SymbolExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType SymbolExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr.type()) << "\n";
    out << "        Symbol " << expr.symbol()._name << "\n";
    out << "    }}\n";

    if (expr.hasVar()) {
        const auto& decl = expr.var();
        out << "    _" << std::hex << &expr << " ||--o{{ VAR_" << decl.id() << " : _\n";
    }
}

void CypherASTDumper::dump(std::ostream& out, const LiteralExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType LiteralExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr.type()) << "\n";

    const auto& literal = expr.literal();

    const auto& l = literal.value();
    if (std::holds_alternative<std::monostate>(l)) {
        out << "        NullLiteral _\n";
    } else if (const auto* v = std::get_if<bool>(&l)) {
        out << "        BoolLiteral " << *v << "\n";
    } else if (const auto* v = std::get_if<int64_t>(&l)) {
        out << "        IntLiteral _" << *v << "\n";
    } else if (const auto* v = std::get_if<double>(&l)) {
        out << "        DoubleLiteral _" << *v << "\n";
    } else if (const auto* v = std::get_if<std::string_view>(&l)) {
        out << "        StringLiteral " << sanitizeString(*v) << "\n";
    } else if (const auto* v = std::get_if<char>(&l)) {
        out << "        CharLiteral _" << *v << "\n";
    } else if (const auto* v = std::get_if<MapLiteral*>(&l)) {
        out << "        MapLiteral _" << std::hex << v << "\n";
    } else {
        throw ASTException("Unknown literal type");
    }

    out << "    }}\n";
}

void CypherASTDumper::dump(std::ostream& out, const ParameterExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType ParameterExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr.type()) << "\n";
    out << "        Parameter " << sanitizeString(expr.param()._name) << "\n";
    out << "    }}\n";
}

void CypherASTDumper::dump(std::ostream& out, const PathExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType PathExpression\n";
    out << "    }}\n";
}

void CypherASTDumper::dump(std::ostream& out, const NodeLabelExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType NodeLabelExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(EvaluatedType::Bool) << "\n";

    const auto& label = expr.labelNames();
    for (const auto& l : label) {
        out << "        Label " << l << "\n";
    }

    out << "    }}\n";

    if (!expr.hasDecl()) {
        return;
    }

    out << "    _" << std::hex << &expr << " ||--o{{ VAR_" << expr.decl().id() << " : _\n";
}

void CypherASTDumper::dump(std::ostream& out, const StringExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType StringExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(EvaluatedType::Bool) << "\n";

    switch (expr.getStringOperator()) {
        case StringOperator::StartsWith:
            out << "        Operator STARTS_WITH\n";
            break;
        case StringOperator::EndsWith:
            out << "        Operator ENDS_WITH\n";
            break;
        case StringOperator::Contains:
            out << "        Operator CONTAINS\n";
            break;
        default:
            throw ASTException("Unknown string operator");
            break;
    }

    out << "    }}\n";

    const auto& left = expr.left();
    const auto& right = expr.right();

    out << "    _" << std::hex << &expr << " ||--o{{ _" << std::hex << &right << " : _\n";

    out << "    _" << std::hex << &expr << " ||--o{{ _" << std::hex << &left << " : _\n";

    dump(out, left);
    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const PropertyExpression& expr) {
    out << "    _" << std::hex << &expr << " {{\n";
    out << "        ASTType PropertyExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr.type()) << "\n";

    out << "        QualifiedName ";

    size_t i = 0;
    for (const auto& name : expr.name()) {
        if (i != 0) {
            out << "_";
        }

        out << name;
        i++;
    }

    out << "\n    }}\n";

    if (!expr.hasDecl()) {
        return;
    }

    out << "    _" << std::hex << &expr << " ||--o{{ VAR_" << expr.decl().id() << " : _\n";
}

void CypherASTDumper::dump(std::ostream& out, const VarDecl& decl) {
    if (_dumpedVariables.contains(decl.id())) {
        return;
    }

    _dumpedVariables.insert(decl.id());

    out << "    VAR_" << decl.id() << " {{\n";
    out << "        ValueType " << EvaluatedTypeName::value(decl.type()) << "\n";

    const auto& name = decl.name();

    if (!name.empty()) {
        out << "        Name " << name << "\n";
    }

    out << "    }}\n";
}
