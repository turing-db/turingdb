#include "Stmt.h"

#include "SinglePartQuery.h"
#include "CallSubqueryStmt.h"
#include "NextSegmentStmt.h"

using namespace db;

Stmt::~Stmt() {
}

bool Stmt::isUpdating(Kind kind) {
    switch (kind) {
        case Kind::CREATE:
        case Kind::MERGE:
        case Kind::SET:
        case Kind::REMOVE:
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
        case Kind::NEXT_SEGMENT:
            return false;
        break;
    }

    return false;
}

const CallSubqueryStmt* Stmt::getSubqueryBody(const Stmt* stmt) {
    const Kind kind = stmt->getKind();

    if (kind == Kind::CALL_SUBQUERY) {
        return static_cast<const CallSubqueryStmt*>(stmt);
    } else if (kind == Kind::NEXT_SEGMENT) {
        return static_cast<const NextSegmentStmt*>(stmt)->getBody();
    }

    return nullptr;
}

bool Stmt::isUpdating(const Stmt* stmt) {
    if (const CallSubqueryStmt* subquery = getSubqueryBody(stmt)) {
        return !subquery->isReturning() && subquery->writesToTheGraph();
    }

    return isUpdating(stmt->getKind());
}

bool Stmt::writesToTheGraph(const Stmt* stmt) {
    if (const CallSubqueryStmt* subquery = getSubqueryBody(stmt)) {
        return subquery->writesToTheGraph();
    }

    return isUpdating(stmt->getKind());
}

bool Stmt::isReading(const Stmt* stmt) {
    if (getSubqueryBody(stmt)) {
        return readsTheGraph(stmt);
    }

    return !isUpdating(stmt->getKind());
}

bool Stmt::readsTheGraph(const Stmt* stmt) {
    const Kind kind = stmt->getKind();

    switch (kind) {
        case Kind::CALL_SUBQUERY:
        case Kind::NEXT_SEGMENT:
            return getSubqueryBody(stmt)->readsTheGraph();
        break;

        case Kind::MATCH:
        case Kind::CALL:
        case Kind::SHORTESTPATH:
        case Kind::LOAD_CSV:
        case Kind::VECTOR_SEARCH:
            return true;
        break;

        // A MERGE matches the writes buffered above it as well as the graph, so it is
        // hidden from nothing
        case Kind::CREATE:
        case Kind::MERGE:
        case Kind::SET:
        case Kind::REMOVE:
        case Kind::DELETE:
        case Kind::UNWIND:
        case Kind::WITH:
        case Kind::RETURN:
            return false;
        break;
    }

    return false;
}
