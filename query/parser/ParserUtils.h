#pragma once

#include <vector>

namespace db {

class CypherAST;
class EmbeddingLiteral;
class ListExpr;

class ParserUtils {
public:
    static void listExprToFloatVector(const ListExpr* list, std::vector<float>& out);

    static EmbeddingLiteral* listExprToEmbeddingLiteral(CypherAST* ast, const ListExpr* list);
};

}
