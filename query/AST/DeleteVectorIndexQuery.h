#pragma once

#include "QueryCommand.h"

#include <string_view>

namespace db {

class CypherAST;
class DeclContext;

class DeleteVectorIndexQuery : public QueryCommand {
public:
    static DeleteVectorIndexQuery* create(CypherAST* ast, std::string_view indexName);

    Kind getKind() const override { return Kind::DELETE_VECTOR_INDEX_QUERY; }

    std::string_view getIndexName() const { return _indexName; }

private:
    std::string_view _indexName;

    DeleteVectorIndexQuery(DeclContext* declContext, std::string_view indexName);
    ~DeleteVectorIndexQuery() override;
};

}
