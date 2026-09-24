#include "ParserUtils.h"

#include <bit>
#include <float.h>
#include <stdlib.h>

#include <spdlog/fmt/bundled/format.h>

#include "expr/BinaryExpr.h"
#include "expr/ExistsExpr.h"
#include "expr/ListExpr.h"
#include "expr/LiteralExpr.h"
#include "stmt/CallStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/SetStmt.h"
#include "stmt/StmtContainer.h"
#include "CypherAST.h"
#include "Literal.h"
#include "NodePattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "SinglePartQuery.h"
#include "SourceManager.h"
#include "WhereClause.h"
#include "ParserException.h"

using namespace db;

namespace {

bool isChainableComparison(BinaryOperator op) {
    return op == BinaryOperator::Equal
           || op == BinaryOperator::NotEqual
           || op == BinaryOperator::LessThan
           || op == BinaryOperator::GreaterThan
           || op == BinaryOperator::LessThanOrEqual
           || op == BinaryOperator::GreaterThanOrEqual;
}

}

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

void ParserUtils::foldEntityWheres(CypherAST* ast, Pattern* pattern) {
    Expr* conjunction = nullptr;

    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const WhereClause* entityWhere = entity->getWhere();
            if (!entityWhere) {
                continue;
            }

            Expr* predicate = entityWhere->getExpr();

            if (conjunction) {
                conjunction = BinaryExpr::create(ast, BinaryOperator::And, conjunction, predicate);
            } else {
                conjunction = predicate;
            }
        }
    }

    if (!conjunction) {
        return;
    }

    WhereClause* where = pattern->getWhere();

    if (where) {
        where->setExpr(BinaryExpr::create(ast, BinaryOperator::And, conjunction, where->getExpr()));
    } else {
        pattern->setWhere(WhereClause::create(ast, conjunction));
    }
}

NodePattern* ParserUtils::createNodePattern(CypherAST* ast,
                                            Symbol* symbol,
                                            SymbolChain* labels,
                                            MapLiteral* properties,
                                            WhereClause* where) {
    NodePattern* node = NodePattern::create(ast);
    node->setSymbol(symbol);
    node->setLabels(labels);
    node->setProperties(properties);
    node->setWhere(where);

    return node;
}

SinglePartQuery* ParserUtils::createPatternBody(CypherAST* ast,
                                               Pattern* pattern,
                                               const SourceLocation& location) {
    SourceManager* sourceManager = ast->getSourceManager();

    MatchStmt* match = MatchStmt::create(ast, pattern);
    sourceManager->setLocation(match, location);

    StmtContainer* stmts = StmtContainer::create(ast);
    stmts->add(match);
    sourceManager->setLocation(stmts, location);

    SinglePartQuery* body = SinglePartQuery::create(ast);
    body->setStmts(stmts);
    sourceManager->setLocation(body, location);

    return body;
}

ExistsExpr* ParserUtils::createPatternPredicate(CypherAST* ast,
                                                PatternElement* element,
                                                const SourceLocation& location) {
    Pattern* pattern = Pattern::create(ast);
    pattern->addElement(element);
    foldEntityWheres(ast, pattern);

    SinglePartQuery* body = createPatternBody(ast, pattern, location);

    ExistsExpr* predicate = ExistsExpr::create(ast, {body});
    predicate->setPredicatePattern(pattern);
    ast->getSourceManager()->setLocation(predicate, location);

    return predicate;
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

void ParserUtils::markStandaloneCall(const SinglePartQuery* query) {
    if (query->getReturnStmt()) {
        return;
    }

    const StmtContainer* stmts = query->getStmts();
    if (!stmts) {
        return;
    }

    const StmtContainer::Stmts& statements = stmts->stmts();
    if (statements.size() != 1) {
        return;
    }

    Stmt* stmt = statements.front();
    if (stmt->getKind() != Stmt::Kind::CALL) {
        return;
    }

    static_cast<CallStmt*>(stmt)->setStandaloneCall(true);
}

void ParserUtils::startComparisonChain(CypherAST* ast,
                                       ComparisonChain& chain,
                                       Expr* lhs,
                                       BinaryOperator op,
                                       Expr* rhs,
                                       const SourceLocation& rhsLocation,
                                       const SourceLocation& chainLocation) {
    chain._expr = BinaryExpr::create(ast, op, lhs, rhs);
    chain._rightOperand = isChainableComparison(op) ? rhs : nullptr;
    chain._rightOperandLocation = rhsLocation;

    ast->getSourceManager()->setLocation(chain._expr, chainLocation);
}

void ParserUtils::extendComparisonChain(CypherAST* ast,
                                        ComparisonChain& chain,
                                        BinaryOperator op,
                                        Expr* rhs,
                                        const SourceLocation& rhsLocation,
                                        const SourceLocation& chainLocation) {
    SourceManager* const sourceManager = ast->getSourceManager();
    const bool chains = isChainableComparison(op);

    // IN and the two IS tests take a right side of their own, so what stands to their left
    // is the comparison before them rather than the operand it ended on
    if (!chains || !chain._rightOperand) {
        chain._expr = BinaryExpr::create(ast, op, chain._expr, rhs);
        chain._rightOperand = chains ? rhs : nullptr;
        chain._rightOperandLocation = rhsLocation;

        sourceManager->setLocation(chain._expr, chainLocation);
        return;
    }

    // A link spans the two operands it compares, not the chain that ended on the left
    // one: a diagnostic about `2 < 'a'` of `1 < 2 < 'a'` underlines that much
    SourceLocation comparisonLocation = chain._rightOperandLocation;
    comparisonLocation._endLine = rhsLocation._endLine;
    comparisonLocation._endColumn = rhsLocation._endColumn;

    BinaryExpr* const comparison = BinaryExpr::create(ast, op, chain._rightOperand, rhs);
    sourceManager->setLocation(comparison, comparisonLocation);

    chain._expr = BinaryExpr::createComparisonChain(ast, chain._expr, comparison);
    chain._rightOperand = rhs;
    chain._rightOperandLocation = rhsLocation;

    sourceManager->setLocation(chain._expr, chainLocation);
}
