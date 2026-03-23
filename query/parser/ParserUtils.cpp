#include "ParserUtils.h"

#include <limits>
#include <math.h>

#include <spdlog/fmt/bundled/format.h>

#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "Literal.h"
#include "ParserException.h"

using namespace db;

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
            const int64_t val = intLit->getValue();
            const int64_t floatLimit = static_cast<int64_t>(floorf(std::numeric_limits<float>::max()));
            if (val > floatLimit || val < -floatLimit) {
                throw ParserException(fmt::format("Integer {} exceeds float representable range", val));
            }
            out.push_back(static_cast<float>(val));
        }
    }
}

EmbeddingLiteral* ParserUtils::listExprToEmbeddingLiteral(CypherAST* ast, const ListExpr* list) {
    if (list->empty()) {
        throw ParserException("Empty embedding literals are not supported");
    }

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
