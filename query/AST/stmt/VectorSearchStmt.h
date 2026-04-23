#pragma once

#include "Stmt.h"

#include <stdint.h>
#include <string_view>

namespace db {

class CypherAST;
class ListLiteral;
class YieldClause;

class VectorSearchStmt : public Stmt {
public:
    static VectorSearchStmt* create(CypherAST* ast);

    Kind getKind() const override { return Kind::VECTOR_SEARCH; }

    void setIndexName(std::string_view name) { _indexName = name; }
    void setK(uint64_t k) { _k = k; }
    void setQueryVector(ListLiteral* vec) { _queryVector = vec; }
    void setYield(YieldClause* yield) { _yield = yield; }

    std::string_view getIndexName() const { return _indexName; }
    uint64_t getK() const { return _k; }
    ListLiteral* getQueryVector() const { return _queryVector; }
    YieldClause* getYield() const { return _yield; }

private:
    std::string_view _indexName;
    uint64_t _k {10};
    ListLiteral* _queryVector {nullptr};
    YieldClause* _yield {nullptr};

    VectorSearchStmt();
    ~VectorSearchStmt() override;
};

}
