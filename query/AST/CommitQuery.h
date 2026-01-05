#pragma once

#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;
class DeclContext;

class CommitQuery : public QueryCommand {
public:
    static CommitQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::COMMIT_QUERY; }

private:
    CommitQuery(DeclContext* declContext);
    ~CommitQuery() override;
};

}
