#pragma once

#include "Stmt.h"

namespace db::v2 {

class FunctionInvocationExpr;
class YieldClause;

class CallStmt : public Stmt {
public:
    static CallStmt* create(CypherAST* ast);

    void setFunc(FunctionInvocationExpr* func) { _func = func; }
    void setOptional(bool optional) { _optional = optional; }
    void setYield(YieldClause* yield) { _yield = yield; }

    FunctionInvocationExpr* getFunc() const { return _func; }

    bool isOptional() const { return _optional; }
    Kind getKind() const override { return Kind::CALL; }
    YieldClause* getYield() const { return _yield; }

private:
    FunctionInvocationExpr* _func {nullptr};
    YieldClause* _yield {nullptr};
    bool _optional {false};

    CallStmt();
    ~CallStmt() override;
};

}
