#pragma once

namespace db::v2 {

class CypherAST;
class DeclContext;

class QueryCommand {
public:
    friend CypherAST;

    DeclContext* getDeclContext() const { return _declContext; }

protected:
    DeclContext* _declContext {nullptr};

    QueryCommand();
    virtual ~QueryCommand();
};

}
