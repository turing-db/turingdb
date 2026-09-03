#include "ParserUtils.h"

#include <bit>
#include <float.h>
#include <stdlib.h>

#include <spdlog/fmt/bundled/format.h>

#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "stmt/SetStmt.h"
#include "Literal.h"
#include "ParserException.h"

using namespace db;

void ParserUtils::listExprToFloatVector(const ListLiteral* list, std::vector<float>& out) {
    out.clear();
    out.reserve(list->size());
    for (Expr* elem : list->items()) {
        const auto* litExpr = static_cast<const LiteralExpr*>(elem);
        const Literal* lit = litExpr->getLiteral();
        if (lit->getKind() == Literal::Kind::DOUBLE) {
            const auto* doubleLit = static_cast<const DoubleLiteral*>(lit);
            out.push_back(static_cast<float>(doubleLit->getValue()));
        } else if (lit->getKind() == Literal::Kind::INTEGER) {
            const auto* intLit = static_cast<const IntegerLiteral*>(lit);
            const int64_t val = intLit->getValue();
            const uint64_t absVal = static_cast<uint64_t>(llabs(val));
            const int sigBits = 64 - std::countl_zero(absVal) - std::countr_zero(absVal);
            if (sigBits > FLT_MANT_DIG) {
                throw ParserException(fmt::format("Integer {} cannot be exactly represented as float", val));
            }
            out.push_back(static_cast<float>(val));
        }
    }
}

void ParserUtils::mergeSetClauses(SetStmt*& held, SetStmt* addition) {
    if (!addition) {
        return;
    }

    if (!held) {
        held = addition;
        return;
    }

    for (SetItem* item : addition->getItems()) {
        held->addItem(item);
    }
}

EmbeddingLiteral* ParserUtils::listExprToEmbeddingLiteral(CypherAST* ast, const ListLiteral* list) {
    if (list->empty()) {
        throw ParserException("Empty embedding literals are not supported");
    }

    for (Expr* elem : list->items()) {
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
