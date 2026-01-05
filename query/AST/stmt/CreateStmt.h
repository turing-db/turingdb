#pragma once

#include "Stmt.h"

namespace db {

class Pattern;
class CypherAST;

class CreateStmt : public Stmt {
public:
    static CreateStmt* create(CypherAST* ast, Pattern* pattern);

    Kind getKind() const override { return Kind::CREATE; }

    const Pattern* getPattern() const { return _pattern; }

private:
    Pattern* _pattern {nullptr};

    CreateStmt(Pattern* pattern)
        : _pattern(pattern)
    {
    }

    ~CreateStmt() override;
};

}
