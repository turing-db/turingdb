#pragma once

#include "Stmt.h"

namespace db {

class Pattern;
class SetStmt;
class CypherAST;

// MERGE's one path pattern, plus the two SET clauses its outcome selects between:
// ON CREATE ... runs over the rows the pattern was written for, ON MATCH ... over the
// rows it was found for.
class MergeStmt : public Stmt {
public:
    static MergeStmt* create(CypherAST* ast, Pattern* pattern);

    Kind getKind() const override { return Kind::MERGE; }

    const Pattern* getPattern() const { return _pattern; }

    const SetStmt* getOnCreate() const { return _onCreate; }
    const SetStmt* getOnMatch() const { return _onMatch; }

    void setOnCreate(SetStmt* onCreate) { _onCreate = onCreate; }
    void setOnMatch(SetStmt* onMatch) { _onMatch = onMatch; }

private:
    Pattern* _pattern {nullptr};
    SetStmt* _onCreate {nullptr};
    SetStmt* _onMatch {nullptr};

    MergeStmt(Pattern* pattern)
        : _pattern(pattern)
    {
    }

    ~MergeStmt() override;
};

}
