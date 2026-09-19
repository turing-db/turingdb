#pragma once

#include <vector>

namespace db {

class CypherAST;
class EmbeddingLiteral;
class ListLiteral;
class SetStmt;
class SinglePartQuery;

class ParserUtils {
public:
    ParserUtils() = delete;
    ~ParserUtils() = delete;

    static EmbeddingLiteral* listExprToEmbeddingLiteral(CypherAST* ast, const ListLiteral* list);

    // Folds a repeated ON CREATE / ON MATCH clause into the one already held for that
    // outcome, so a MERGE keeps a single SET clause per branch
    static void mergeSetClauses(SetStmt*& held, SetStmt* addition);

    // A query that is one CALL and nothing else has no projection of its own, so what the
    // call yields is the result it reports. A subquery body is not such a query: it is a
    // clause of the query around it, and the RETURN it owes is its own
    static void markStandaloneCall(const SinglePartQuery* query);

private:
    static void listExprToFloatVector(const ListLiteral* list, std::vector<float>& out);
};

}
