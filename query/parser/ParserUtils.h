#pragma once

#include <vector>

namespace db {

class CypherAST;
class EmbeddingLiteral;
class ListLiteral;
class SetStmt;

class ParserUtils {
public:
    ParserUtils() = delete;
    ~ParserUtils() = delete;

    static EmbeddingLiteral* listExprToEmbeddingLiteral(CypherAST* ast, const ListLiteral* list);

    // Folds a repeated ON CREATE / ON MATCH clause into the one already held for that
    // outcome, so a MERGE keeps a single SET clause per branch
    static void mergeSetClauses(SetStmt*& held, SetStmt* addition);

private:
    static void listExprToFloatVector(const ListLiteral* list, std::vector<float>& out);
};

}
