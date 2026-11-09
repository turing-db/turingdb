#pragma once

#include "Stmt.h"

#include <string_view>

namespace db {

class CypherAST;
class Expr;
class YieldClause;

class VectorSearchStmt : public Stmt {
public:
    static VectorSearchStmt* create(CypherAST* ast);

    Kind getKind() const override { return Kind::VECTOR_SEARCH; }

    void setIndexName(std::string_view name) { _indexName = name; }
    void setK(Expr* k) { _k = k; }
    void setQueryVector(Expr* queryVector) { _queryVector = queryVector; }
    void setYield(YieldClause* yield) { _yield = yield; }

    std::string_view getIndexName() const { return _indexName; }
    Expr* getK() const { return _k; }
    Expr* getQueryVector() const { return _queryVector; }
    YieldClause* getYield() const { return _yield; }

private:
    std::string_view _indexName;
    Expr* _k {nullptr};
    Expr* _queryVector {nullptr};
    YieldClause* _yield {nullptr};

    VectorSearchStmt();
    ~VectorSearchStmt() override;
};

}
