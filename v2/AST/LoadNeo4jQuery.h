#pragma once

#include <string_view>

#include "Path.h"
#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;
class DeclContext;

class LoadNeo4jQuery : public QueryCommand {
public:
    static LoadNeo4jQuery* create(CypherAST* ast,
                                  fs::Path&& path);

    Kind getKind() const override { return Kind::LOAD_NEO4J_QUERY; }

    void setGraphName(std::string_view graphName) { _graphName = graphName; }

    const fs::Path& getFilePath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }

private:
    fs::Path _path;
    std::string_view _graphName;

    LoadNeo4jQuery(DeclContext* declContext, fs::Path&& path);
    ~LoadNeo4jQuery() override;
};

}
