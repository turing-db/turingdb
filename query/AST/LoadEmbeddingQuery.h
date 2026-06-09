#pragma once

#include "QueryCommand.h"

#include <string_view>

namespace db {

class CypherAST;
class DeclContext;

class LoadEmbeddingQuery : public QueryCommand {
public:
    static LoadEmbeddingQuery* create(CypherAST* ast,
                                      std::string_view filePath,
                                      std::string_view propertyName);

    Kind getKind() const override { return Kind::LOAD_EMBEDDING_QUERY; }

    std::string_view getFilePath() const { return _filePath; }
    std::string_view getPropertyName() const { return _propertyName; }

private:
    std::string_view _filePath;
    std::string_view _propertyName;

    LoadEmbeddingQuery(DeclContext* declContext,
                       std::string_view filePath,
                       std::string_view propertyName);
    ~LoadEmbeddingQuery() override;
};

}
