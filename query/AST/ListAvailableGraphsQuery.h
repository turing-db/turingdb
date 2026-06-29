#pragma once

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class ListAvailableGraphsQuery : public QueryCommand {
public:
    static ListAvailableGraphsQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::LIST_AVAILABLE_GRAPHS_QUERY; }

private:
    ListAvailableGraphsQuery(DeclContext* declContext);
    ~ListAvailableGraphsQuery() override;
};

}
