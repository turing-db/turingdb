#pragma once

#include <string_view>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class LoadCommitQuery : public QueryCommand {
public:
    static LoadCommitQuery* create(CypherAST* ast, std::string_view hashStr);

    Kind getKind() const override { return Kind::LOAD_COMMIT_QUERY; }

    std::string_view getHashStr() const { return _hashStr; }

private:
    std::string_view _hashStr;

    LoadCommitQuery(DeclContext* declContext, std::string_view hashStr);
    ~LoadCommitQuery() override;
};

}
