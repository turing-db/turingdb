#pragma once

#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;

class SinglePartQuery : public QueryCommand {
public:
    static SinglePartQuery* create(CypherAST* ast);

private:
    SinglePartQuery();
    ~SinglePartQuery() override;
};


}
