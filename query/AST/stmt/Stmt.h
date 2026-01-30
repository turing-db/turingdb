#pragma once

namespace db {

class CypherAST;

class Stmt {
public:
    friend CypherAST;

    enum class Kind {
        MATCH = 0,
        CALL,
        CREATE,
        SET,
        DELETE,
        RETURN,
        SHORTESTPATH,
    };

    virtual Kind getKind() const = 0;

protected:
    Stmt() = default;
    virtual ~Stmt();
};

}
