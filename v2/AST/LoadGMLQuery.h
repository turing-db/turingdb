#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;
class DeclContext;

class LoadGMLQuery : public QueryCommand {
public:
    static LoadGMLQuery* create(CypherAST* ast,
                                std::string_view graphName,
                                std::string_view filePath);

    Kind getKind() const override { return Kind::LOAD_GML_QUERY; }

    std::string_view getGraphName() const { return _graphName; }

private:
    std::string_view _graphName;
    std::string_view _filePath;

    LoadGMLQuery(DeclContext* declContext,
                 std::string_view graphName,
                 std::string_view filePath);
    ~LoadGMLQuery() override;
};

}
