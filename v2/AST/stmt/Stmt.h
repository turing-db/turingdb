#pragma once

namespace db::v2 {

class CypherAST;

class Stmt {
public:
    friend CypherAST;

    enum class Kind {
        MATCH = 0,
        CREATE,
        DELETE,
        RETURN
    };

    virtual Kind getKind() const = 0;

protected:
    Stmt() = default;
    virtual ~Stmt();
};

}
