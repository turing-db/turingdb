#pragma once

namespace db::v2 {

class CypherAST;

class QueryCommand {
public:
    friend CypherAST;

protected:
    QueryCommand();
    virtual ~QueryCommand();
};

}
