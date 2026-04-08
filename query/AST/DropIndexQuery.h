#pragma once

#include <string_view>

#include "CypherAST.h"
#include "QueryCommand.h"

namespace db {

class DropIndexQuery final : public QueryCommand {
public:
    static DropIndexQuery* create(CypherAST* ast, std::string_view indexName);

    std::string_view indexName() const { return _indexName; }

    Kind getKind() const final { return QueryCommand::Kind::DROP_INDEX_QUERY; }

private:
    DropIndexQuery(DeclContext* declCtxt, std::string_view indexName);
    std::string_view _indexName;
};

}
