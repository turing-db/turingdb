#include "ConstantExpressionDetector.h"

#include "Literal.h"
#include "Symbol.h"

#include "BinaryExpr.h"
#include "Expr.h"
#include "ListExpr.h"
#include "LiteralExpr.h"
#include "StringExpr.h"
#include "UnaryExpr.h"

using namespace db;

// An expression is constant when it is written entirely out of values the query text
// carries: it reads no row and no context, so every row of the query gives it the same
// value. The answer is conservative - an expression this cannot take apart is reported
// as varying, which only ever costs the caller the work it would have avoided
bool ConstantExpressionDetector::isConstant(const Expr* expr) {
    if (!expr) {
        return false;
    }

    const Expr::Kind kind = expr->getKind();

    switch (kind) {
        case Expr::Kind::LITERAL: {
            const LiteralExpr* literalExpr = static_cast<const LiteralExpr*>(expr);

            return isConstantLiteral(literalExpr->getLiteral());
        }
        break;

        case Expr::Kind::BINARY: {
            const BinaryExpr* binary = static_cast<const BinaryExpr*>(expr);

            return isConstant(binary->getLHS()) && isConstant(binary->getRHS());
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unary = static_cast<const UnaryExpr*>(expr);

            return isConstant(unary->getSubExpr());
        }
        break;

        case Expr::Kind::STRING: {
            const StringExpr* string = static_cast<const StringExpr*>(expr);

            return isConstant(string->getLHS()) && isConstant(string->getRHS());
        }
        break;

        case Expr::Kind::LIST: {
            const ListExpr* list = static_cast<const ListExpr*>(expr);

            return areAllConstant(list->getElements());
        }
        break;

        default:
            // A symbol, a property, an entity type test and a path all read the row they
            // are evaluated on, and an index expression reads a CSV row. A function
            // invocation is left out too: even one written over constant arguments may
            // aggregate over the rows or answer differently on every call
            return false;
        break;
    }
}

bool ConstantExpressionDetector::isConstantLiteral(const Literal* literal) {
    if (!literal) {
        return false;
    }

    const Literal::Kind kind = literal->getKind();

    switch (kind) {
        case Literal::Kind::LIST:
            // A list is written element by element, and an element is an expression of
            // its own: [1, n.age] varies with the property it holds
            return areAllConstant(static_cast<const ListLiteral*>(literal)->items());
        break;

        case Literal::Kind::MAP: {
            const MapLiteral* map = static_cast<const MapLiteral*>(literal);

            // The keys of a map are symbols written in the query, so only its values can
            // make it vary
            for (const std::pair<Symbol* const, Expr*>& entry : *map) {
                if (!isConstant(entry.second)) {
                    return false;
                }
            }

            return true;
        }
        break;

        case Literal::Kind::WILDCARD:
            // The * of COUNT(*) stands for the rows themselves rather than for a value
            return false;
        break;

        default:
            // Every other literal is one value, written in the query text
            return true;
        break;
    }
}

bool ConstantExpressionDetector::areAllConstant(std::span<Expr* const> exprs) {
    for (const Expr* expr : exprs) {
        if (!isConstant(expr)) {
            return false;
        }
    }

    return true;
}
