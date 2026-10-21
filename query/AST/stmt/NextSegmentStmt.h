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

    // What the scope clause of a CALL around a segment that returns nothing imports,
    // declared again after it: the segment's table is gone, the CALL's row is not
    const std::vector<const VarDecl*>& getCarriedImports() const { return _carriedImports; }

    void addCarriedImport(const VarDecl* decl) { _carriedImports.push_back(decl); }

private:
    CallSubqueryStmt* _body {nullptr};
    std::vector<const VarDecl*> _carriedImports;

    explicit NextSegmentStmt(CallSubqueryStmt* body);
    ~NextSegmentStmt() final;
};

}
