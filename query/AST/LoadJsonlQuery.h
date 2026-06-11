#pragma once

#include <string_view>
#include <unordered_map>

#include "Path.h"
#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class LoadJsonlQuery : public QueryCommand {
public:
    static LoadJsonlQuery* create(CypherAST* ast,
                                  fs::Path&& path);

    Kind getKind() const override { return Kind::LOAD_JSONL_QUERY; }

    void setGraphName(std::string_view graphName) { _graphName = graphName; }
    void setEmbeddingSpecs(std::unordered_map<std::string_view, size_t>&& specs) {
        _embeddingSpecs = std::move(specs);
    }

    const fs::Path& getFilePath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }
    const std::unordered_map<std::string_view, size_t>& getEmbeddingSpecs() const {
        return _embeddingSpecs;
    }

private:
    fs::Path _path;
    std::string_view _graphName;
    std::unordered_map<std::string_view, size_t> _embeddingSpecs;

    LoadJsonlQuery(DeclContext* declContext, fs::Path&& path);
    ~LoadJsonlQuery() override;
};

}
