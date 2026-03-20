#pragma once

#include <vector>

namespace db {

class CypherAST;
class EmbeddingLiteral;
class ListExpr;

class ParserUtils {
public:
    ParserUtils() = delete;
    ~ParserUtils() = delete;

    static EmbeddingLiteral* listExprToEmbeddingLiteral(CypherAST* ast, const ListExpr* list);

private:
    static void listExprToFloatVector(const ListExpr* list, std::vector<float>& out);
};

}
