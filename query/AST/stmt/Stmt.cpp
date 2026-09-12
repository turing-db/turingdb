#include "Stmt.h"

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
            return false;
        break;
    }

    return false;
}
