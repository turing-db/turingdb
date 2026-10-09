#pragma once

#include <vector>

#include "expr/Operators.h"
#include "stmt/CallSubqueryStmt.h"
#include "SourceLocation.h"
#include "UnionQuery.h"

namespace db {

class CypherAST;
class EmbeddingLiteral;
class EntityTypeExpr;
class ExistsExpr;
class Expr;
class ListLiteral;
class MapLiteral;
class NodePattern;
class Pattern;
class PatternElement;
class PropertyExpr;
class SetStmt;
class SinglePartQuery;
class StmtContainer;
class Symbol;
class SymbolChain;
class WhereClause;

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

    // `MATCH (a WHERE p)-[r WHERE q]->(b) WHERE w` filters as `WHERE p AND q AND w`
    static void foldEntityWheres(CypherAST* ast, Pattern* pattern);

    static NodePattern* createNodePattern(CypherAST* ast,
                                          Symbol* symbol,
                                          SymbolChain* labels,
                                          MapLiteral* properties,
                                          WhereClause* where);

    // `{ WHEN ... }` combined by a UNION is `CALL () { WHEN ... } RETURN <its columns>`.
    // False when a column of the first branch has no name to return it under: a WHEN
    // combined by a UNION names each column with AS.
    static bool createWhenOperand(CypherAST* ast,
                                  const CallSubqueryStmt::Branches& branches,
                                  const SourceLocation& location,
                                  UnionQuery::Branch& operand);

    // `FOREACH (x IN list | updates)` is `CALL (*) { UNWIND list AS x updates }`
    static CallSubqueryStmt* createForeach(CypherAST* ast,
                                           Symbol* variable,
                                           Expr* list,
                                           StmtContainer* updates,
                                           const SourceLocation& location);

    static SinglePartQuery* createPatternBody(CypherAST* ast,
                                              Pattern* pattern,
                                              const SourceLocation& location);

    static ExistsExpr* createPatternPredicate(CypherAST* ast,
                                              PatternElement* element,
                                              const SourceLocation& location);

    static PropertyExpr* createParenthesizedPropertyAccess(CypherAST* ast,
                                                           Expr* base,
                                                           Symbol* propertyName);

    static EntityTypeExpr* createLabelTest(CypherAST* ast, Expr* operand, SymbolChain* labels);

    // `-1` stays the literal it spells: an embedding list or a procedure's constant argument
    // takes literals only
    static Expr* createNegation(CypherAST* ast, Expr* operand);

    // `(a)--(b)` is `(a) - -(b)`: between a node written in parentheses, or a pattern, and
    // the negation of another, the two minus signs are an undirected edge
    static Expr* createSubtraction(CypherAST* ast,
                                   Expr* lhs,
                                   Expr* rhs,
                                   const SourceLocation& location);

    // `[x IN xs]` is the comprehension that copies xs, not a list holding one IN test
    static Expr* createListOrComprehension(CypherAST* ast, ListLiteral* list);

    // nullptr where the head is neither `variable IN list` nor a pattern with a projection
    static Expr* createComprehension(CypherAST* ast,
                                     Expr* head,
                                     WhereClause* where,
                                     Expr* projection,
                                     const SourceLocation& headLocation);

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
