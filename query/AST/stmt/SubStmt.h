#pragma once

namespace db {

class CypherAST;

class SubStmt {
public:
    friend CypherAST;

protected:
    SubStmt() = default;
    virtual ~SubStmt() = default;

    SubStmt(const SubStmt&) = default;
    SubStmt& operator=(const SubStmt&) = default;
    SubStmt(SubStmt&&) = default;
    SubStmt& operator=(SubStmt&&) = default;
};

}
