#pragma once

#include "columns/ColumnOperator.h"
#include "expr/Operators.h"

namespace db {

class Column;
class LocalMemory;
class ExprProgram;
class PredicateProgramGenerator;
class Predicate;
class Expr;
class VarDecl;
class UnaryExpr;
class BinaryExpr;
class PropertyExpr;
class LiteralExpr;
class SymbolExpr;
class PipelineGenerator;
class PendingOutputView;

class ExprProgramGenerator {
public:
    friend PredicateProgramGenerator;

    ExprProgramGenerator(PipelineGenerator* gen,
                         ExprProgram* exprProg,
                         const PendingOutputView& pendingOut)
        : _gen(gen),
        _exprProg(exprProg),
        _pendingOut(pendingOut)
    {
    }

    virtual ~ExprProgramGenerator() = default;

    ExprProgramGenerator(const ExprProgramGenerator&) = delete;
    ExprProgramGenerator& operator=(const ExprProgramGenerator&) = delete;
    ExprProgramGenerator(ExprProgramGenerator&&) = delete;
    ExprProgramGenerator& operator=(ExprProgramGenerator&&) = delete;

    Column* registerPropertyConstraint(const Expr* expr);

private:
    PipelineGenerator* _gen {nullptr};
    ExprProgram* _exprProg {nullptr};
    const PendingOutputView& _pendingOut;

    Column* generateExpr(const Expr* expr);
    Column* generateUnaryExpr(const UnaryExpr* expr);
    Column* generateBinaryExpr(const BinaryExpr* expr);
    Column* generatePropertyExpr(const PropertyExpr* propExpr);
    Column* generateLiteralExpr(const LiteralExpr* literalExpr);
    Column* generateSymbolExpr(const SymbolExpr* symbolExpr);

    Column* allocResultColumn(const Expr* expr);
    Column* allocResCol(ColumnOperator op, const Column* lhs, const Column* rhs);

    static ColumnOperator unaryOperatorToColumnOperator(UnaryOperator op);
    static ColumnOperator binaryOperatorToColumnOperator(BinaryOperator op);
};

}
