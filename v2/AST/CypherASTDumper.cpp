#include "CypherASTDumper.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"
#include "stmt/StmtContainer.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/Limit.h"
#include "stmt/Skip.h"
#include "Projection.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "WhereClause.h"
#include "NodePattern.h"
#include "EdgePattern.h"
#include "Symbol.h"
#include "expr/Literal.h"
#include "decl/VarDecl.h"
#include "QualifiedName.h"

#include "expr/All.h"

#include "ASTException.h"

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

CypherASTDumper::CypherASTDumper(const CypherAST* ast)
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

    for (const auto& query : _ast->queries()) {
        if (const SinglePartQuery* q = dynamic_cast<const SinglePartQuery*>(query)) {
            dump(out, q);
            continue;
        }

        throw ASTException("Only single part queries are supported");
    }
}

void CypherASTDumper::dump(std::ostream& out, const SinglePartQuery* query) {
    out << "    script ||--o{{ _" << std::hex << &query << " : _\n";
    out << "    _" << std::hex << &query << " {{\n";
    out << "        ASTType SinglePartQuery\n";
    out << "    }}\n";

    auto dumpContainer = [&](const StmtContainer* container) {
        for (const auto& st : container->stmts()) {
            if (const MatchStmt* matchSt = dynamic_cast<const MatchStmt*>(st)) {
                out << "    _" << std::hex << &query << " ||--o{{ _" << std::hex << matchSt << " : _\n";

                dump(out, matchSt);
            } else {
                throw ASTException("Unknown statement type");
            }
        }
    };

    if (const StmtContainer* readStmts = query->getReadStmts()) {
        dumpContainer(readStmts);
    }

    if (const StmtContainer* updateStmts = query->getUpdateStmts()) {
        dumpContainer(updateStmts);
    }

    if (const ReturnStmt* retSt = query->getReturnStmt()) {
        out << "    _" << std::hex << query << " ||--o{{ _" << std::hex << retSt << " : _\n";
        dump(out, retSt);
    }
}

void CypherASTDumper::dump(std::ostream& out, const MatchStmt* match) {
    out << "    _" << std::hex << match << " {{\n";

    if (match->isOptional()) {
        out << "        ASTType OPTIONAL_MATCH\n";
    } else {
        out << "        ASTType MATCH\n";
    }

    out << "    }}\n";

    const Pattern* pattern = match->getPattern();
    out << "    _" << std::hex << match << " ||--o{{ _" << std::hex << pattern << " : _\n";

    dump(out, pattern);

    if (match->hasLimit()) {
        const Limit* lim = match->getLimit();
        out << "    _" << std::hex << match << " ||--o{{ _" << std::hex << lim << " : _\n";
        dump(out, lim);
    }

    if (match->hasSkip()) {
        const Skip* skip = match->getSkip();
        out << "    _" << std::hex << match << " ||--o{{ _" << std::hex << skip << " : _\n";
        dump(out, skip);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Limit* lim) {
    out << "    _" << std::hex << lim << " {{\n";
    out << "        ASTType LIMIT\n";
    out << "    }}\n";

    const Expr* expr = lim->getExpr();

    out << "    _" << std::hex << lim << " ||--o{{ _" << std::hex << expr << " : _\n";

    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const Skip* skip) {
    out << "    _" << std::hex << skip << " {{\n";
    out << "        ASTType SKIP\n";
    out << "    }}\n";

    const Expr* expr = skip->getExpr();

    out << "    _" << std::hex << skip << " ||--o{{ _" << std::hex << expr << " : _\n";

    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const ReturnStmt* ret) {
    out << "    _" << std::hex << ret << " {{\n";
    out << "        ASTType RETURN\n";
    out << "    }}\n";

    const Projection* projection = ret->getProjection();

    out << "    _" << std::hex << ret << " ||--o{{ _" << std::hex << projection << " : _\n";

    dump(out, projection);
}

void CypherASTDumper::dump(std::ostream& out, const Projection* projection) {
    out << "    _" << std::hex << projection << " {{\n";
    out << "        ASTType Projection\n";

    if (projection->isDistinct()) {
        out << "        Distinct true\n";
    }

    if (projection->hasLimit()) {
        const Limit* limit = projection->getLimit();
        out << "        Limit _" << std::hex << limit << "\n";
    }

    if (projection->hasSkip()) {
        const Skip* skip = projection->getSkip();
        out << "        Skip _" << std::hex << skip << "\n";
    }


    if (projection->isAll()) {
        out << "        ProjectAll true\n";
    } else {
        const auto& items = projection->items();
        for (const auto& item : items) {
            out << "        Item _" << std::hex << item << "\n";
        }
    }

    out << "    }}\n";

    if (projection->hasLimit()) {
        const Limit* limit = projection->getLimit();
        out << "    _" << std::hex << projection << " ||--o{{ _" << std::hex << limit << " : _\n";
        dump(out, limit);
    }

    if (projection->hasSkip()) {
        const Skip* skip = projection->getSkip();
        out << "    _" << std::hex << projection << " ||--o{{ _" << std::hex << skip << " : _\n";
        dump(out, skip);
    }

    if (projection->isAll()) {
        return;
    }

    const auto& items = projection->items();
    for (const auto& item : items) {
        out << "    _" << std::hex << projection << " ||--o{{ _" << std::hex << item << " : _\n";
        dump(out, item);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Pattern* pattern) {
    out << "    _" << std::hex << pattern << " {{\n";
    out << "        ASTType Pattern\n";
    out << "    }}\n";

    for (const PatternElement* part : pattern->elements()) {
        out << "    _" << std::hex << &pattern << " ||--o{{ _" << std::hex << part << " : _\n";
        dump(out, part);
    }

    if (const WhereClause* where = pattern->getWhere()) {
        out << "    _" << std::hex << pattern << " ||--o{{ _" << std::hex << where << " : _\n";
        dump(out, where);
    }
}

void CypherASTDumper::dump(std::ostream& out, const PatternElement* elem) {
    out << "    _" << std::hex << elem << " {{\n";
    out << "        ASTType PatternElement\n";
    out << "    }}\n";

    for (const auto& entity : elem->getEntities()) {
        out << "    _" << std::hex << elem << " ||--o{{ _" << std::hex << entity << " : _\n";
        if (NodePattern* node = dynamic_cast<NodePattern*>(entity)) {
            dump(out, node);
        } else if (EdgePattern* edge = dynamic_cast<EdgePattern*>(entity)) {
            dump(out, edge);
        }
    }
}

void CypherASTDumper::dump(std::ostream& out, const WhereClause* where) {
    out << "    _" << std::hex << where << " {{\n";
    out << "        ASTType WHERE\n";
    out << "    }}\n";

    const Expr* expr = where->getExpr();
    out << "    _" << std::hex << where << " ||--o{{ _" << std::hex << expr << " : _\n";
    dump(out, expr);
}

void CypherASTDumper::dump(std::ostream& out, const NodePattern* node) {
    out << "    _" << std::hex << node << " {{\n";
    out << "        ASTType NodePattern\n";

    if (const Symbol* symbol = node->getSymbol()) {
        out << "        Symbol " << symbol->getName() << "\n";
    }

    for (const Symbol* type : node->labels()) {
        out << "        Label " << type->getName() << "\n";
    }

    if (const MapLiteral* properties = node->getProperties()) {
        for (const auto& [k, v] : *properties) {
            out << "        prop_" << sanitizeString(k->getName()) << " _" << std::hex << v << "\n";
        }
    }

    out << "    }}\n";

    const VarDecl* decl = node->getDecl();
    if (!decl) {
        return;
    }

    out << "    _" << std::hex << node << " ||--o{{ VAR_" << decl << " : _\n";
    dump(out, decl);
}

void CypherASTDumper::dump(std::ostream& out, const EdgePattern* edge) {
    out << "    _" << std::hex << edge << " {{\n";
    out << "        ASTType EdgePattern\n";

    if (Symbol* symbol = edge->getSymbol()) {
        out << "        Name " << symbol->getName() << "\n";
    }

    for (const Symbol* type : edge->types()) {
            out << "        EdgeType " << type << "\n";
    }

    if (const MapLiteral* properties = edge->getProperties()) {
        for (const auto& [k, v] : *properties) {
            out << "        prop_" << sanitizeString(k->getName()) << " _" << std::hex << v << "\n";
        }
    }

    out << "    }}\n";

    const VarDecl* decl = edge->getDecl();
    if (!decl) {
        return;
    }

    out << "    _" << std::hex << edge << " ||--o{{ VAR_" << decl << " : _\n";
    dump(out, decl);
}

void CypherASTDumper::dump(std::ostream& out, const MapLiteral* map) {
    out << "    _" << std::hex << map << " {{\n";
    out << "        ASTType MapLiteral\n";

    for (const auto& [k, v] : *map) {
        out << "        prop_" << sanitizeString(k->getName()) << " _" << std::hex << v << "\n";
    }

    out << "    }}\n";

    for (const auto& [k, v] : *map) {
        out << "    _" << std::hex << map << " ||--o{{ _" << std::hex << v << " : _\n";
        dump(out, v);
    }
}

void CypherASTDumper::dump(std::ostream& out, const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            dump(out, dynamic_cast<const BinaryExpr*>(expr));
            break;
        case Expr::Kind::UNARY:
            dump(out, dynamic_cast<const UnaryExpr*>(expr));
            break;
        case Expr::Kind::STRING:
            dump(out, dynamic_cast<const StringExpr*>(expr));
            break;
        case Expr::Kind::NODE_LABEL:
            dump(out, dynamic_cast<const NodeLabelExpr*>(expr));
            break;
        case Expr::Kind::PROPERTY:
            dump(out, dynamic_cast<const PropertyExpr*>(expr));
            break;
        case Expr::Kind::PATH:
            dump(out, dynamic_cast<const PathExpr*>(expr));
            break;
        case Expr::Kind::SYMBOL:
            dump(out, dynamic_cast<const SymbolExpr*>(expr));
            break;
        case Expr::Kind::LITERAL:
            dump(out, dynamic_cast<const LiteralExpr*>(expr));
            break;

        default:
            throw ASTException("Unknown expression type");
            break;
    }
}

void CypherASTDumper::dump(std::ostream& out, const BinaryExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType BinaryExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    switch (expr->getOperator()) {
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

    const Expr* left = expr->getLHS();
    const Expr* right = expr->getRHS();

    out << "    _" << std::hex << expr << " ||--o{{ _" << std::hex << left << " : _\n";

    dump(out, left);

    out << "    _" << std::hex << expr << " ||--o{{ _" << std::hex << right << " : _\n";

    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const UnaryExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType UnaryExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    switch (expr->getOperator()) {
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

    const Expr* right = expr->getSubExpr();

    out << "    _" << std::hex << expr << " ||--o{{ _" << std::hex << right << " : _\n";

    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const SymbolExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType SymbolExpression\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";
    out << "        Symbol " << expr->getSymbol()->getName() << "\n";
    out << "    }}\n";

    if (VarDecl* decl = expr->getDecl()) {
        out << "    _" << std::hex << expr << " ||--o{{ VAR_" << decl << " : _\n";
    }
}

void CypherASTDumper::dump(std::ostream& out, const LiteralExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType LiteralExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    const Literal* literal = expr->getLiteral();
    switch (literal->getKind()) {
        case Literal::Kind::NULL_LITERAL:
        {
            out << "        NullLiteral _\n";
            break;
        }
        case Literal::Kind::BOOL:
        {
            const BoolLiteral* boolLiteral = dynamic_cast<const BoolLiteral*>(literal);
            out << "        BoolLiteral " << boolLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::INTEGER:
        {
            const IntegerLiteral* integerLiteral = dynamic_cast<const IntegerLiteral*>(literal);
            out << "        IntLiteral " << integerLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::DOUBLE:
        {
            const DoubleLiteral* doubleLiteral = dynamic_cast<const DoubleLiteral*>(literal);
            out << "        DoubleLiteral " << doubleLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::STRING:
        {
            const StringLiteral* stringLiteral = dynamic_cast<const StringLiteral*>(literal);
            out << "        StringLiteral " << stringLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::CHAR:
        {
            const CharLiteral* charLiteral = dynamic_cast<const CharLiteral*>(literal);
            out << "        CharLiteral " << charLiteral->getValue() << "\n";
            break;
        }
        case Literal::Kind::MAP:
        {
            const MapLiteral* mapLiteral = dynamic_cast<const MapLiteral*>(literal);
            out << "        MapLiteral " << mapLiteral << "\n";
            break;
        }
        default:
        {
            throw ASTException("Unknown literal type");
            break;
        }
    }

    out << "    }}\n";
}

void CypherASTDumper::dump(std::ostream& out, const PathExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType PathExpr\n";
    out << "    }}\n";
}

void CypherASTDumper::dump(std::ostream& out, const NodeLabelExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType NodeLabelExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(EvaluatedType::Bool) << "\n";

    const auto& labels = expr->labels();
    for (const auto& label : labels) {
        out << "        Label " << label->getName() << "\n";
    }

    out << "    }}\n";

    if (!expr->getDecl()) {
        return;
    }

    out << "    _" << std::hex << expr << " ||--o{{ VAR_" << expr->getDecl() << " : _\n";
}

void CypherASTDumper::dump(std::ostream& out, const StringExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType StringExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(EvaluatedType::Bool) << "\n";

    switch (expr->getStringOperator()) {
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

    const Expr* left = expr->getLHS();
    const Expr* right = expr->getRHS();

    out << "    _" << std::hex << expr << " ||--o{{ _" << std::hex << right << " : _\n";

    out << "    _" << std::hex << expr << " ||--o{{ _" << std::hex << left << " : _\n";

    dump(out, left);
    dump(out, right);
}

void CypherASTDumper::dump(std::ostream& out, const PropertyExpr* expr) {
    out << "    _" << std::hex << expr << " {{\n";
    out << "        ASTType PropertyExpr\n";
    out << "        ValueType " << EvaluatedTypeName::value(expr->getType()) << "\n";

    out << "        QualifiedName ";

    size_t i = 0;
    for (const Symbol* name : expr->getName()->names()) {
        if (i != 0) {
            out << "_";
        }

        out << name->getName();
        i++;
    }

    out << "\n    }}\n";

    if (!expr->getDecl()) {
        return;
    }

    out << "    _" << std::hex << expr << " ||--o{{ VAR_" << expr->getDecl() << " : _\n";
}

void CypherASTDumper::dump(std::ostream& out, const VarDecl* decl) {
    out << "    VAR_" << decl << " {{\n";
    out << "        ValueType " << EvaluatedTypeName::value(decl->getType()) << "\n";

    const auto& name = decl->getName();

    if (!name.empty()) {
        out << "        Name " << name << "\n";
    }

    out << "    }}\n";
}
