#include "StructuralExpressionComparator.h"

#include <algorithm>

#include "FunctionInvocation.h"
#include "Literal.h"
#include "QualifiedName.h"
#include "Symbol.h"

#include "BinaryExpr.h"
#include "Expr.h"
#include "ExprChain.h"
#include "FunctionInvocationExpr.h"
#include "IndexExpr.h"
#include "ListExpr.h"
#include "LiteralExpr.h"
#include "PropertyExpr.h"
#include "StringExpr.h"
#include "SymbolExpr.h"
#include "UnaryExpr.h"

using namespace db;

bool StructuralExpressionComparator::equal(const Expr* lhs, const Expr* rhs) {
    // The same node, or two null operands of the same shape, need no further comparison
    if (lhs == rhs) {
        return true;
    }

    if (!lhs || !rhs) {
        return false;
    }

    const Expr::Kind kind = lhs->getKind();
    if (kind != rhs->getKind()) {
        return false;
    }

    switch (kind) {
        case Expr::Kind::BINARY: {
            const BinaryExpr* lhsBinary = static_cast<const BinaryExpr*>(lhs);
            const BinaryExpr* rhsBinary = static_cast<const BinaryExpr*>(rhs);

            const bool sameOperator = lhsBinary->getOperator() == rhsBinary->getOperator();

            return sameOperator
                   && equal(lhsBinary->getLHS(), rhsBinary->getLHS())
                   && equal(lhsBinary->getRHS(), rhsBinary->getRHS());
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* lhsUnary = static_cast<const UnaryExpr*>(lhs);
            const UnaryExpr* rhsUnary = static_cast<const UnaryExpr*>(rhs);

            const bool sameOperator = lhsUnary->getOperator() == rhsUnary->getOperator();

            return sameOperator && equal(lhsUnary->getSubExpr(), rhsUnary->getSubExpr());
        }
        break;

        case Expr::Kind::STRING: {
            const StringExpr* lhsString = static_cast<const StringExpr*>(lhs);
            const StringExpr* rhsString = static_cast<const StringExpr*>(rhs);

            const bool sameOperator = lhsString->getStringOperator() == rhsString->getStringOperator();

            return sameOperator
                   && equal(lhsString->getLHS(), rhsString->getLHS())
                   && equal(lhsString->getRHS(), rhsString->getRHS());
        }
        break;

        case Expr::Kind::PROPERTY: {
            const PropertyExpr* lhsProperty = static_cast<const PropertyExpr*>(lhs);
            const PropertyExpr* rhsProperty = static_cast<const PropertyExpr*>(rhs);

            const bool sameEntity = lhsProperty->getEntityVarDecl() == rhsProperty->getEntityVarDecl();
            const bool sameProperty = lhsProperty->getPropName() == rhsProperty->getPropName();

            return sameEntity && sameProperty;
        }
        break;

        case Expr::Kind::SYMBOL: {
            const SymbolExpr* lhsSymbol = static_cast<const SymbolExpr*>(lhs);
            const SymbolExpr* rhsSymbol = static_cast<const SymbolExpr*>(rhs);

            return lhsSymbol->getDecl() == rhsSymbol->getDecl();
        }
        break;

        case Expr::Kind::LITERAL: {
            const LiteralExpr* lhsLiteral = static_cast<const LiteralExpr*>(lhs);
            const LiteralExpr* rhsLiteral = static_cast<const LiteralExpr*>(rhs);

            return equalLiterals(lhsLiteral->getLiteral(), rhsLiteral->getLiteral());
        }
        break;

        case Expr::Kind::INDEX: {
            const IndexExpr* lhsIndex = static_cast<const IndexExpr*>(lhs);
            const IndexExpr* rhsIndex = static_cast<const IndexExpr*>(rhs);

            return equal(lhsIndex->getBase(), rhsIndex->getBase())
                   && equal(lhsIndex->getIndexExpr(), rhsIndex->getIndexExpr());
        }
        break;

        case Expr::Kind::LIST: {
            const ListExpr* lhsList = static_cast<const ListExpr*>(lhs);
            const ListExpr* rhsList = static_cast<const ListExpr*>(rhs);

            return equalExprLists(lhsList->getElements(), rhsList->getElements());
        }
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* lhsCall = static_cast<const FunctionInvocationExpr*>(lhs);
            const FunctionInvocationExpr* rhsCall = static_cast<const FunctionInvocationExpr*>(rhs);

            return equalInvocations(lhsCall->getFunctionInvocation(),
                                    rhsCall->getFunctionInvocation());
        }
        break;

        default:
            // ENTITY_TYPES and PATH carry a symbol chain and a pattern, neither of which
            // this comparator takes apart, so such an expression equals only itself
            return false;
        break;
    }
}

bool StructuralExpressionComparator::equalLiterals(const Literal* lhs, const Literal* rhs) {
    if (lhs == rhs) {
        return true;
    }

    if (!lhs || !rhs) {
        return false;
    }

    const Literal::Kind kind = lhs->getKind();
    if (kind != rhs->getKind()) {
        return false;
    }

    switch (kind) {
        case Literal::Kind::NULL_LITERAL:
            // Neither null nor wildcard carries a value, so every one is the same one
            return true;
        break;

        case Literal::Kind::WILDCARD:
            return true;
        break;

        case Literal::Kind::BOOL:
            return static_cast<const BoolLiteral*>(lhs)->getValue()
                   == static_cast<const BoolLiteral*>(rhs)->getValue();
        break;

        case Literal::Kind::INTEGER:
            return static_cast<const IntegerLiteral*>(lhs)->getValue()
                   == static_cast<const IntegerLiteral*>(rhs)->getValue();
        break;

        case Literal::Kind::DOUBLE:
            // A NaN literal compares equal to nothing, itself included, which leaves it
            // on the conservative side of the contract
            return static_cast<const DoubleLiteral*>(lhs)->getValue()
                   == static_cast<const DoubleLiteral*>(rhs)->getValue();
        break;

        case Literal::Kind::STRING:
            return static_cast<const StringLiteral*>(lhs)->getValue()
                   == static_cast<const StringLiteral*>(rhs)->getValue();
        break;

        case Literal::Kind::CHAR:
            return static_cast<const CharLiteral*>(lhs)->getValue()
                   == static_cast<const CharLiteral*>(rhs)->getValue();
        break;

        case Literal::Kind::EMBEDDING:
            return std::ranges::equal(static_cast<const EmbeddingLiteral*>(lhs)->getValue(),
                                      static_cast<const EmbeddingLiteral*>(rhs)->getValue());
        break;

        case Literal::Kind::LIST:
            return equalExprLists(static_cast<const ListLiteral*>(lhs)->items(),
                                  static_cast<const ListLiteral*>(rhs)->items());
        break;

        default:
            // A map literal keys its entries by Symbol identity, so two maps written the
            // same way do not compare equal entry by entry
            return false;
        break;
    }
}

bool StructuralExpressionComparator::equalInvocations(const FunctionInvocation* lhs,
                                                      const FunctionInvocation* rhs) {
    if (lhs == rhs) {
        return true;
    }

    if (!lhs || !rhs) {
        return false;
    }

    const QualifiedName* lhsName = lhs->getName();
    const QualifiedName* rhsName = rhs->getName();
    if (!lhsName || !rhsName || lhsName->size() != rhsName->size()) {
        return false;
    }

    for (size_t index = 0; index < lhsName->size(); index++) {
        if (lhsName->get(index)->getName() != rhsName->get(index)->getName()) {
            return false;
        }
    }

    // A call with no argument list has none to compare, so two of them match on name
    const ExprChain* lhsArguments = lhs->getArguments();
    const ExprChain* rhsArguments = rhs->getArguments();
    if (!lhsArguments || !rhsArguments) {
        return lhsArguments == rhsArguments;
    }

    return equalExprLists(lhsArguments->getExprs(), rhsArguments->getExprs());
}

bool StructuralExpressionComparator::equalExprLists(std::span<Expr* const> lhs,
                                                    std::span<Expr* const> rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }

    for (size_t index = 0; index < lhs.size(); index++) {
        if (!equal(lhs[index], rhs[index])) {
            return false;
        }
    }

    return true;
}
