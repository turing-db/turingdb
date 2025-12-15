#pragma once

#include "columns/ColumnOperator.h"
#include "expr/Operators.h"
namespace db {
class Column;
class LocalMemory;
}

namespace db::v2 {

class ExprProgram;
class Predicate;
class Expr;
class VarDecl;
class UnaryExpr;
class BinaryExpr;
class PropertyExpr;
class LiteralExpr;
class PipelineGenerator;
class PendingOutputView;
class LabelSet;

class ExprProgramGenerator {
public:
    ExprProgramGenerator(PipelineGenerator* gen, ExprProgram* exprProg,
                         const PendingOutputView& pendingOut)
        : _gen(gen),
        _exprProg(exprProg),
        _pendingOut(pendingOut)
    {
    }

    ~ExprProgramGenerator() = default;

    ExprProgramGenerator(const ExprProgramGenerator&) = delete;
    ExprProgramGenerator& operator=(const ExprProgramGenerator&) = delete;
    ExprProgramGenerator(ExprProgramGenerator&&) = delete;
    ExprProgramGenerator& operator=(ExprProgramGenerator&&) = delete;

    void generatePredicate(const Predicate* pred);
    void addLabelConstraint(const LabelSet& lblset);

private:
    PipelineGenerator* _gen;
    ExprProgram* _exprProg {nullptr};
    const PendingOutputView& _pendingOut;

    Column* generateExpr(const Expr* expr);
    Column* generateUnaryExpr(const UnaryExpr* expr);
    Column* generateBinaryExpr(const BinaryExpr* expr);
    Column* allocResultColumn(const Expr* expr);
    Column* generatePropertyExpr(const PropertyExpr* propExpr);
    Column* generateLiteralExpr(const LiteralExpr* literalExpr);

    static ColumnOperator unaryOperatorToColumnOperator(UnaryOperator op);
    static ColumnOperator binaryOperatorToColumnOperator(BinaryOperator op);
};

}
