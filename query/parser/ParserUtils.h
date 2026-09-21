#pragma once

#include <vector>

#include "expr/Operators.h"
#include "SourceLocation.h"

namespace db {

class CypherAST;
class EmbeddingLiteral;
class Expr;
class ListLiteral;
class SetStmt;
class SinglePartQuery;

class ParserUtils {
public:
    // openCypher reads `a < b <= c` as `a < b AND b <= c`, so a chain being built carries
    // the operand the last sign ended on: the next sign compares against that one
    struct ComparisonChain {
        Expr* _expr {nullptr};
        Expr* _rightOperand {nullptr};
        SourceLocation _rightOperandLocation;
    };

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

    static void startComparisonChain(CypherAST* ast,
                                     ComparisonChain& chain,
                                     Expr* lhs,
                                     BinaryOperator op,
                                     Expr* rhs,
                                     const SourceLocation& rhsLocation,
                                     const SourceLocation& chainLocation);

    static void extendComparisonChain(CypherAST* ast,
                                      ComparisonChain& chain,
                                      BinaryOperator op,
                                      Expr* rhs,
                                      const SourceLocation& rhsLocation,
                                      const SourceLocation& chainLocation);

private:
    static void listExprToFloatVector(const ListLiteral* list, std::vector<float>& out);
};

}
