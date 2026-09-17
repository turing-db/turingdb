#include "Stmt.h"

#include "SinglePartQuery.h"
#include "CallSubqueryStmt.h"

using namespace db;

Stmt::~Stmt() {
}

bool Stmt::isUpdating(Kind kind) {
    switch (kind) {
        case Kind::CREATE:
        case Kind::MERGE:
        case Kind::SET:
        case Kind::DELETE:
            return true;
        break;

        case Kind::MATCH:
        case Kind::CALL:
        case Kind::RETURN:
        case Kind::SHORTESTPATH:
        case Kind::LOAD_CSV:
        case Kind::VECTOR_SEARCH:
        case Kind::UNWIND:
        case Kind::WITH:
        case Kind::CALL_SUBQUERY:
            return false;
        break;
    }

    return false;
}

bool Stmt::isUpdating(const Stmt* stmt) {
    const Kind kind = stmt->getKind();

    if (kind == Kind::CALL_SUBQUERY) {
        const CallSubqueryStmt* subquery = static_cast<const CallSubqueryStmt*>(stmt);

        return !subquery->isReturning() && subquery->getBody()->writesToTheGraph();
    }

    return isUpdating(kind);
}
