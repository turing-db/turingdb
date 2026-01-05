#pragma once

#include "Stmt.h"

namespace db::v2 {

class ExprChain;
class CypherAST;

class DeleteStmt : public Stmt {
public:
    static DeleteStmt* create(CypherAST* ast, ExprChain* exprChain);

    Kind getKind() const override { return Kind::DELETE; }

    bool isDetaching() const { return _detaching; }
    void setDetaching(bool detach) { _detaching = detach; }

    const ExprChain* getExpressions() const { return _exprs; }

private:
    ExprChain* _exprs {nullptr};
    bool _detaching {false};

    DeleteStmt(ExprChain* exprs)
        : _exprs(exprs)
    {
    }

    ~DeleteStmt() override;
};

}
