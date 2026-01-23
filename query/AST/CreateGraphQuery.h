#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class CreateGraphQuery : public QueryCommand {
public:
    static CreateGraphQuery* create(CypherAST* ast, std::string_view name);

    Kind getKind() const override { return Kind::CREATE_GRAPH_QUERY; }
    std::string_view getGraphName() const { return _graphName; };

private:
    std::string_view _graphName;

    CreateGraphQuery(DeclContext* declContext, std::string_view name);
    ~CreateGraphQuery() override;
};

}
