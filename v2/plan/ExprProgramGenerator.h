#pragma once

#include <unordered_map>

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

class ExprProgramGenerator {
public:
    ExprProgramGenerator(LocalMemory* mem, ExprProgram* exprProg)
        : _mem(mem),
        _exprProg(exprProg)
    {
    }

    void generatePredicate(const Predicate* pred);

private:
    LocalMemory* _mem {nullptr};
    ExprProgram* _exprProg {nullptr};
    std::unordered_map<const VarDecl*, Column*> _propColumnMap;
    Column* _rootColumn {nullptr};

    Column* generateExpr(const Expr* expr);
    Column* generateBinaryExpr(const BinaryExpr* expr);
    Column* allocResultColumn(const Expr* expr);
    Column* generatePropertyExpr(const PropertyExpr* propExpr);
    Column* generateLiteralExpr(const LiteralExpr* literalExpr);
};

}
