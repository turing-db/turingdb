#pragma once

namespace db::v2 {

class CypherAST;

class Stmt {
public:
    friend CypherAST;

protected:
    Stmt() = default;
    virtual ~Stmt();
};

}
