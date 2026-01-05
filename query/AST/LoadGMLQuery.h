#pragma once

#include <string_view>

#include "QueryCommand.h"

#include "Path.h"

namespace db {

class CypherAST;
class DeclContext;

class LoadGMLQuery : public QueryCommand {
public:
    static LoadGMLQuery* create(CypherAST* ast,
                                fs::Path&& filePath);

    Kind getKind() const override { return Kind::LOAD_GML_QUERY; }

    std::string_view getGraphName() const { return _graphName; }

    void setGraphName(std::string_view name) { _graphName = name; }

    const fs::Path& getFilePath() const { return _filePath; }

private:
    fs::Path _filePath;
    std::string_view _graphName;

    LoadGMLQuery(DeclContext* declContext,
                 fs::Path&& filePath);
    ~LoadGMLQuery() override;
};

}
