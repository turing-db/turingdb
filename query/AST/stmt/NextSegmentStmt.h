#pragma once

#include <vector>

#include "Stmt.h"

namespace db {

class CallSubqueryStmt;
class CypherAST;
class VarDecl;

// A UNION or a WHEN that is one segment of a NEXT chain, or a segment ending on an
// updating clause that NEXT follows. It reads the rows the segments before it returned
// and its RETURN replaces them. A UNION reads the whole table in every branch; a WHEN
// decides per row, as the CALL (*) { WHEN ... } its body is written as would.
class NextSegmentStmt final : public Stmt {
public:
    static NextSegmentStmt* create(CypherAST* ast, CallSubqueryStmt* body);

    Kind getKind() const final { return Kind::NEXT_SEGMENT; }

    CallSubqueryStmt* getBody() const { return _body; }

    // The imports of the body run once per row around the segment that the segment drops,
    // declared again behind it: each holds one value for the whole run
    const std::vector<const VarDecl*>& redeclaredImports() const { return _redeclaredImports; }

    void addRedeclaredImport(const VarDecl* decl) { _redeclaredImports.push_back(decl); }

private:
    CallSubqueryStmt* _body {nullptr};
    std::vector<const VarDecl*> _redeclaredImports;

    explicit NextSegmentStmt(CallSubqueryStmt* body);
    ~NextSegmentStmt() final;
};

}
