#pragma once

#include "Stmt.h"

namespace db {

class CypherAST;
class Expr;
class YieldClause;

class VectorSearchStmt : public Stmt {
public:
    static VectorSearchStmt* create(CypherAST* ast);

    Kind getKind() const override { return Kind::VECTOR_SEARCH; }

    void setIndex(Expr* index) { _index = index; }
    void setK(Expr* k) { _k = k; }
    void setQueryVector(Expr* queryVector) { _queryVector = queryVector; }
    void setYield(YieldClause* yield) { _yield = yield; }

    Expr* getIndex() const { return _index; }
    Expr* getK() const { return _k; }
    Expr* getQueryVector() const { return _queryVector; }
    YieldClause* getYield() const { return _yield; }

private:
    Expr* _index {nullptr};
    Expr* _k {nullptr};
    Expr* _queryVector {nullptr};
    YieldClause* _yield {nullptr};

    VectorSearchStmt();
    ~VectorSearchStmt() override;
};

}
