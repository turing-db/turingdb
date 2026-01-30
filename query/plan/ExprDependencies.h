#pragma once

#include <vector>

#include "PlanGraphVariables.h"

namespace db {

class PlanGraphNode;
class Expr;
class FunctionInvocationExpr;
class PlanGraphTopology;
class VarNode;

class ExprDependencies {
public:
    struct VarDependency {
        PlanGraphNode* _producerNode {nullptr};
        Expr* _expr {nullptr};
    };

    struct FuncDependency {
        const FunctionInvocationExpr* _expr {nullptr};
    };

    using VarDepVector = std::vector<VarDependency>;
    using FuncDepVector = std::vector<FuncDependency>;

    ExprDependencies();
    ~ExprDependencies();

    const VarDepVector& getVarDeps() const { return _varDeps; }

    VarDepVector& getVarDeps() { return _varDeps; }

    const FuncDepVector& getFuncDeps() const { return _funcDeps; }

    bool empty() const {
        return _varDeps.empty() && _funcDeps.empty();
    }

    void genExprDependencies(PlanGraphVariables& variables, Expr* expr);

    VarNode* findCommonSuccessor(PlanGraphTopology* topology, VarNode* var) const;

private:
    VarDepVector _varDeps;
    FuncDepVector _funcDeps;
};

}
