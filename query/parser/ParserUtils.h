#pragma once

#include <vector>

namespace db {

class CypherAST;
class EmbeddingLiteral;
class ListLiteral;

class ParserUtils {
public:
    ParserUtils() = delete;
    ~ParserUtils() = delete;

    static EmbeddingLiteral* listExprToEmbeddingLiteral(CypherAST* ast, const ListLiteral* list);

private:
    static void listExprToFloatVector(const ListLiteral* list, std::vector<float>& out);
};

}
