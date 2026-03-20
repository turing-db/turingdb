#include "ParserUtils.h"

#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"
#include "ParserException.h"

namespace db {

void ParserUtils::listExprToFloatVector(const ListExpr* list, std::vector<float>& out) {
    out.clear();
    out.reserve(list->size());
    for (Expr* elem : *list) {
        const auto* litExpr = static_cast<const LiteralExpr*>(elem);
        const Literal* lit = litExpr->getLiteral();
        if (lit->getKind() == Literal::Kind::DOUBLE) {
            const auto* doubleLit = static_cast<const DoubleLiteral*>(lit);
            out.push_back(static_cast<float>(doubleLit->getValue()));
        } else if (lit->getKind() == Literal::Kind::INTEGER) {
            const auto* intLit = static_cast<const IntegerLiteral*>(lit);
            out.push_back(static_cast<float>(intLit->getValue()));
        }
    }
}

EmbeddingLiteral* ParserUtils::listExprToEmbeddingLiteral(CypherAST* ast, const ListExpr* list) {
    for (Expr* elem : *list) {
        if (elem->getKind() != Expr::Kind::LITERAL) {
            throw ParserException("Non-literal list elements are not supported");
        }

        const Literal* lit = static_cast<const LiteralExpr*>(elem)->getLiteral();
        const auto litKind = lit->getKind();
        if (litKind != Literal::Kind::DOUBLE && litKind != Literal::Kind::INTEGER) {
            throw ParserException("Non-numeric list elements are not supported");
        }
    }

    std::vector<float> data;
    listExprToFloatVector(list, data);
    return EmbeddingLiteral::create(ast, std::move(data));
}

}
