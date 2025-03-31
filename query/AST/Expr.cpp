#include "Expr.h"

#include "ASTContext.h"

using namespace db;

Expr::~Expr() {
}

void Expr::postCreate(ASTContext* ctxt) {
    ctxt->addExpr(this);
}

// VarExpr
VarExpr::VarExpr(const std::string& varName)
    : _varName(varName)
{
}

VarExpr::~VarExpr() {
}

VarExpr* VarExpr::create(ASTContext* ctxt, const std::string& varName) {
    VarExpr* expr = new VarExpr(varName);
    expr->postCreate(ctxt);
    return expr;
}

ExprConst::~ExprConst() {
}

DoubleExprConst* DoubleExprConst::create(ASTContext* ctxt, double val){
    DoubleExprConst* expr = new DoubleExprConst(val);
    expr->postCreate(ctxt);
    return expr;
}

Int64ExprConst* Int64ExprConst::create(ASTContext* ctxt, int64_t val) {
    Int64ExprConst* expr = new Int64ExprConst(val);
    expr->postCreate(ctxt);
    return expr;
}

UInt64ExprConst* UInt64ExprConst::create(ASTContext* ctxt, uint64_t val) {
    UInt64ExprConst* expr = new UInt64ExprConst(val);
    expr->postCreate(ctxt);
    return expr;
}

StringExprConst* StringExprConst::create(ASTContext* ctxt, const std::string& val){
    StringExprConst* expr = new StringExprConst(val);
    expr->postCreate(ctxt);
    return expr;
}

StringExprConst::~StringExprConst() {
}

BoolExprConst* BoolExprConst::create(ASTContext* ctxt, bool val){
    BoolExprConst* expr = new BoolExprConst(val);
    expr->postCreate(ctxt);
    return expr;
}

BinExpr::BinExpr(Expr* left, Expr* right, OpType opType)
    :_lexpr(left),
    _rexpr(right),
    _opType(opType)
{
}

BinExpr::~BinExpr() {
}

BinExpr* BinExpr::create(ASTContext* ctxt, Expr *left, Expr *right, OpType opType) {
    BinExpr* expr = new BinExpr(left,right, opType);
    expr->postCreate(ctxt);
    return expr;

}
