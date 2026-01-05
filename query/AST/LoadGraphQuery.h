#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;
class DeclContext;

class LoadGraphQuery : public QueryCommand {
public:
    static LoadGraphQuery* create(CypherAST* ast,
                                  std::string_view graphName);

    Kind getKind() const override { return Kind::LOAD_GRAPH_QUERY; }

    std::string_view getGraphName() const { return _graphName; }

private:
    std::string_view _graphName;

    LoadGraphQuery(DeclContext* declContext, std::string_view graphName);
    ~LoadGraphQuery() override;
};

}
