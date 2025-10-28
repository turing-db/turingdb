#pragma once

#include "Stmt.h"

namespace db::v2 {

class Pattern;
class CypherAST;

class SetStmt : public Stmt {
public:
    static SetStmt* create(CypherAST* ast, Pattern* pattern);

    Kind getKind() const override { return Kind::CREATE; }

    const Pattern* getPattern() const { return _pattern; }

private:
    Pattern* _pattern {nullptr};

    SetStmt(Pattern* pattern)
        : _pattern(pattern)
    {
    }

    ~SetStmt() override;
};

}
