#pragma once

#include <vector>

namespace db {

class CypherAST;
class EmbeddingLiteral;
class ListLiteral;
class SetStmt;
class StmtContainer;

class ParserUtils {
public:
    ParserUtils() = delete;
    ~ParserUtils() = delete;

    static EmbeddingLiteral* listExprToEmbeddingLiteral(CypherAST* ast, const ListLiteral* list);

    // Folds a repeated ON CREATE / ON MATCH clause into the one already held for that
    // outcome, so a MERGE keeps a single SET clause per branch
    static void mergeSetClauses(SetStmt*& held, SetStmt* addition);

    // A query that is one CALL and nothing else has no projection of its own, so what the
    // call yields is the result it reports
    static void markStandaloneCall(StmtContainer* stmts);

private:
    static void listExprToFloatVector(const ListLiteral* list, std::vector<float>& out);
};

}
