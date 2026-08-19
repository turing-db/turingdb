#include "ExprChildren.h"

#include "FunctionInvocation.h"

#include "BinaryExpr.h"
#include "Expr.h"
#include "ExprChain.h"
#include "FunctionInvocationExpr.h"
#include "IndexExpr.h"
#include "ListExpr.h"
#include "StringExpr.h"
#include "UnaryExpr.h"

using namespace db;

bool ExprChildren::collect(const Expr* expr, std::vector<const Expr*>& children) {
    children.clear();

    if (!expr) {
        return true;
    }

    switch (expr->getKind()) {
        case Expr::Kind::BINARY: {
            const BinaryExpr* binary = static_cast<const BinaryExpr*>(expr);

            children.push_back(binary->getLHS());
            children.push_back(binary->getRHS());

            return true;
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unary = static_cast<const UnaryExpr*>(expr);

            children.push_back(unary->getSubExpr());

            return true;
        }
        break;

        case Expr::Kind::STRING: {
            const StringExpr* string = static_cast<const StringExpr*>(expr);

            children.push_back(string->getLHS());
            children.push_back(string->getRHS());

            return true;
        }
        break;

        case Expr::Kind::INDEX: {
            const IndexExpr* index = static_cast<const IndexExpr*>(expr);

            children.push_back(index->getBase());
            children.push_back(index->getIndexExpr());

            return true;
        }
        break;

        case Expr::Kind::LIST: {
            const ListExpr* list = static_cast<const ListExpr*>(expr);

            for (const Expr* element : list->getElements()) {
                children.push_back(element);
            }

            return true;
        }
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* call = static_cast<const FunctionInvocationExpr*>(expr);
            const ExprChain* arguments = call->getFunctionInvocation()->getArguments();
            if (!arguments) {
                return true;
            }

            for (const Expr* argument : arguments->getExprs()) {
                children.push_back(argument);
            }

            return true;
        }
        break;

        case Expr::Kind::SYMBOL:
        case Expr::Kind::PROPERTY:
        case Expr::Kind::ENTITY_TYPES:
            // A leaf as far as expressions go: what it reads is a variable, not another
            // expression
            return true;
        break;

        default:
            // A path holds a pattern and a literal may hold a map, neither of which is a
            // list of sub-expressions this can hand back
            return false;
        break;
    }
}
