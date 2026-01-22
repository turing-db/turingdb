#pragma once

#include "QueryCommand.h"

#include <string_view>

namespace db {

class CypherAST;
class DeclContext;

class LoadVectorQuery : public QueryCommand {
public:
    static LoadVectorQuery* create(CypherAST* ast,
                                   std::string_view filePath,
                                   std::string_view indexName);

    Kind getKind() const override { return Kind::LOAD_VECTOR_QUERY; }

    std::string_view getFilePath() const { return _filePath; }
    std::string_view getIndexName() const { return _indexName; }

private:
    std::string_view _filePath;
    std::string_view _indexName;

    LoadVectorQuery(DeclContext* declContext,
                    std::string_view filePath,
                    std::string_view indexName);
    ~LoadVectorQuery() override;
};

}
