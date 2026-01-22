#pragma once

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class ShowVectorIndexesQuery : public QueryCommand {
public:
    static ShowVectorIndexesQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::SHOW_VECTOR_INDEXES_QUERY; }

private:
    ShowVectorIndexesQuery(DeclContext* declContext);
    ~ShowVectorIndexesQuery() override;
};

}
