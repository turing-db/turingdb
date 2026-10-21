#pragma once

#include "Stmt.h"

namespace db {

class CallSubqueryStmt;
class CypherAST;

// A UNION or a WHEN that is one segment of a NEXT chain, or a segment ending on an
// updating clause that NEXT follows. It reads the rows the segments before it returned
// and its RETURN replaces them. A UNION reads the whole table in every branch; a WHEN
// decides per row, as the CALL (*) { WHEN ... } its body is written as would.
class NextSegmentStmt final : public Stmt {
public:
    static NextSegmentStmt* create(CypherAST* ast, CallSubqueryStmt* body);

    Kind getKind() const final { return Kind::NEXT_SEGMENT; }

    CallSubqueryStmt* getBody() const { return _body; }

private:
    CallSubqueryStmt* _body {nullptr};

    explicit NextSegmentStmt(CallSubqueryStmt* body);
    ~NextSegmentStmt() final;
};

}
