#pragma once

#include <vector>

#include "expr/Operators.h"
#include "stmt/CallSubqueryStmt.h"
#include "SourceLocation.h"
#include "UnionQuery.h"
#include "UnicodeNormalForm.h"

namespace db {

class CypherAST;
class EdgePattern;
class EmbeddingLiteral;
class EntityTypeExpr;
class ExistsExpr;
class Expr;
class FunctionInvocation;
class LabelExpr;
class ListLiteral;
class MapLiteral;
class NodePattern;
class Pattern;
class Parameter;
class PatternElement;
class PropertyExpr;
class Projection;
class ReturnStmt;
class SetStmt;
class SinglePartQuery;
class StmtContainer;
class Symbol;
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

    // The queries a NEXT chains, in order: each a single query, the branches of a UNION or
    // those of a WHEN
    using NextSegments = std::vector<CallSubqueryStmt::Branches>;

    ParserUtils() = delete;
    ~ParserUtils() = delete;

    static EmbeddingLiteral* listExprToEmbeddingLiteral(CypherAST* ast, const ListLiteral* list);

    // A parameter written as $0, $1, ... is looked up under the digits as its name
    static Parameter* createNumberedParameter(CypherAST* ast, int64_t number);

    // Folds a repeated ON CREATE / ON MATCH clause into the one already held for that
    // outcome, so a MERGE keeps a single SET clause per branch
    static void mergeSetClauses(SetStmt*& held, SetStmt* addition);

    // A query that is one CALL and nothing else has no projection of its own, so what the
    // call yields is the result it reports. A subquery body is not such a query: it is a
    // clause of the query around it, and the RETURN it owes is its own
    static void markStandaloneCall(const SinglePartQuery* query);

    // `MATCH (a WHERE p)-[r WHERE q]->(b) WHERE w` filters as `WHERE p AND q AND w`
    static void foldEntityWheres(CypherAST* ast, Pattern* pattern);

    // A relationship quantified as [e*1..3] or -[e]->{1,3} becomes the walk repeating it,
    // which takes the quantifier off it; any other relationship is returned as it is
    static EdgePattern* createQuantifiedWalk(CypherAST* ast, EdgePattern* edge, const SourceLocation& location);

    static NodePattern* createNodePattern(CypherAST* ast,
                                          Symbol* symbol,
                                          LabelExpr* labelExpr,
                                          MapLiteral* properties,
                                          WhereClause* where);

    // `{ WHEN ... }` combined by a UNION is `CALL () { WHEN ... } RETURN <its columns>`.
    // False when a column of the first branch has no name to return it under: a WHEN
    // combined by a UNION names each column with AS.
    static bool createWhenOperand(CypherAST* ast,
                                  const CallSubqueryStmt::Branches& branches,
                                  const SourceLocation& location,
                                  UnionQuery::Branch& operand);

    // Whether an item of a segment's RETURN has neither an alias nor a variable to name the
    // column the next segment reads it through
    static bool returnsUnaliasedItem(const CallSubqueryStmt::Branches& segment);

    // `a RETURN x NEXT b` is `a WITH x b`. A UNION or a WHEN segment, and one ending on an
    // updating clause that NEXT follows, is a NextSegmentStmt; ending the chain, its
    // columns are what the query returns.
    static SinglePartQuery* createNextQuery(CypherAST* ast,
                                            const NextSegments& segments,
                                            const SourceLocation& location);

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

    static EntityTypeExpr* createLabelTest(CypherAST* ast, Expr* operand, LabelExpr* labelExpr);

    // `-1` stays the literal it spells: an embedding list or a procedure's constant argument
    // takes literals only
    static Expr* createNegation(CypherAST* ast, Expr* operand);

    // `(a)--(b)` is `(a) - -(b)`: between a node written in parentheses, or a pattern, and
    // the negation of another, the two minus signs are an undirected edge
    static Expr* createSubtraction(CypherAST* ast,
                                   Expr* lhs,
                                   Expr* rhs,
                                   const SourceLocation& location);

    // `normalize(s)` is `normalize(s, NFC)`
    static FunctionInvocation* createNormalizeInvocation(CypherAST* ast, Expr* string, UnicodeNormalForm form);

    // `s IS [NOT] form NORMALIZED` is `[NOT] is_normalized(s, form)`
    static Expr* createIsNormalized(CypherAST* ast,
                                    Expr* string,
                                    UnicodeNormalForm form,
                                    bool negated,
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

    // `RETURN <the columns of @param projection>`, read back by name from the scope the
    // segment that projects them publishes
    static ReturnStmt* createSegmentReturn(CypherAST* ast,
                                           const Projection* projection,
                                           const SourceLocation& location);
};

}
