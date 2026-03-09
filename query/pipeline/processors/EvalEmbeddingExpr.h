#pragma once

namespace db {

class Column;

class EvalEmbeddingExpr {
public:
    static void evalEqual(Column* res, const Column* lhs, const Column* rhs);
    static void evalNotEqual(Column* res, const Column* lhs, const Column* rhs);
    static void evalCosineSimilarity(Column* res, const Column* lhs, const Column* rhs);
};

}
