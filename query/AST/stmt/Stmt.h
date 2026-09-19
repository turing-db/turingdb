#pragma once

namespace db {

class CypherAST;

class Stmt {
public:
    friend CypherAST;

    enum class Kind {
        MATCH = 0,
        CALL,
        CREATE,
        MERGE,
        SET,
        DELETE,
        RETURN,
        SHORTESTPATH,
        LOAD_CSV,
        VECTOR_SEARCH,
        UNWIND,
        WITH,
        CALL_SUBQUERY,
    };

    // A clause that writes to the graph. Cypher orders a query part's clauses reading
    // first and updating last, so this is what tells the two halves of a part apart
    static bool isUpdating(Kind kind);

    // The same over a statement: a CALL subquery is updating when its body writes and ends
    // on no RETURN
    static bool isUpdating(const Stmt* stmt);

    // Whether anything under the clause writes, which a CALL subquery answers from its body
    // whatever the body ends on. A returning body that writes is not an updating clause -
    // its rows join the ones in flight - and it has still written
    static bool writesToTheGraph(const Stmt* stmt);

    virtual Kind getKind() const = 0;

protected:
    Stmt() = default;
    virtual ~Stmt();
};

}
