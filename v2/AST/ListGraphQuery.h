#pragma once

#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;
class DeclContext;

class ListGraphQuery : public QueryCommand {
public:
    static ListGraphQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::LIST_GRAPH_QUERY; }

private:
    ListGraphQuery(DeclContext* declContext);
    ~ListGraphQuery() override;
};

}
