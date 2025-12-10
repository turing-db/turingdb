#pragma once

namespace db {
class Column;
class LocalMemory;
}

namespace db::v2 {

class ExprProgram;
class Predicate;
class Expr;
class VarDecl;
class BinaryExpr;
class PropertyExpr;
class LiteralExpr;
class PipelineGenerator;
class PendingOutputView;

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

private:
    PipelineGenerator* _gen;
    ExprProgram* _exprProg {nullptr};
    const PendingOutputView& _pendingOut;

    Column* generateExpr(const Expr* expr);
    Column* generateBinaryExpr(const BinaryExpr* expr);
    Column* allocResultColumn(const Expr* expr);
    Column* generatePropertyExpr(const PropertyExpr* propExpr);
    Column* generateLiteralExpr(const LiteralExpr* literalExpr);
};

}
