#pragma once

#include <string_view>

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

    const fs::Path& getFilePath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }

private:
    fs::Path _path;
    std::string_view _graphName;

    LoadJsonlQuery(DeclContext* declContext, fs::Path&& path);
    ~LoadJsonlQuery() override;
};

}
