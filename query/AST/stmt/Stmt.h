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
    };

    // A clause that writes to the graph. Cypher orders a query part's clauses reading
    // first and updating last, so this is what tells the two halves of a part apart
    static bool isUpdating(Kind kind);

    virtual Kind getKind() const = 0;

protected:
    Stmt() = default;
    virtual ~Stmt();
};

}
