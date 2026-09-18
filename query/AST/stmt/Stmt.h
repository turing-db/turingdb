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
        REMOVE,
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

    // Whether anything under the clause goes to the graph for rows, which is what an
    // updating clause above it hides its own writes from. A WITH and an UNWIND read the
    // rows in flight rather than the graph, and a CALL subquery answers from its body
    static bool readsTheGraph(const Stmt* stmt);

    // Whether the clause belongs to the reading half of a query part. A CALL subquery
    // joins that half only when its body goes to the graph: one that only writes has
    // nothing an updating clause above it could hide
    static bool isReading(const Stmt* stmt);

    virtual Kind getKind() const = 0;

protected:
    Stmt() = default;
    virtual ~Stmt();
};

}
