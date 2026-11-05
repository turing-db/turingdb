#pragma once

#include <string_view>

#include "Path.h"
#include "QueryCommand.h"

#include "EmbeddingsSpec.h"
#include "TypedPropertySpec.h"

namespace db {

class CypherAST;
class DeclContext;

class LoadJsonlQuery : public QueryCommand {
public:
    static LoadJsonlQuery* create(CypherAST* ast,
                                  fs::Path&& path);

    Kind getKind() const override { return Kind::LOAD_JSONL_QUERY; }

    void setGraphName(std::string_view graphName) { _graphName = graphName; }
    void setEmbeddingSpecs(EmbeddingsSpec&& specs) { _embeddingSpecs = std::move(specs); }
    void setTypedPropertySpec(TypedPropertySpec&& specs) { _typedPropertySpec = std::move(specs); }

    const fs::Path& getFilePath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }
    const EmbeddingsSpec& getEmbeddingSpecs() const {
        return _embeddingSpecs;
    }

    const TypedPropertySpec& getTypedPropertySpec() const { return _typedPropertySpec; }

private:
    fs::Path _path;
    std::string_view _graphName;
    EmbeddingsSpec _embeddingSpecs;
    TypedPropertySpec _typedPropertySpec;

    LoadJsonlQuery(DeclContext* declContext, fs::Path&& path);
    ~LoadJsonlQuery() override;
};

}
