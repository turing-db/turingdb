#include "ExprTree.h"

#include "CypherAST.h"
#include "Expr.h"

using namespace db::v2;

ExprTree* ExprTree::create(CypherAST* ast, Expr* expr) {
    auto* tree = new ExprTree();
    ast->addExprTree(tree);
    tree->addExpr(expr);

    return tree;
}
