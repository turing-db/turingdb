#include <gtest/gtest.h>

#include <stdint.h>
#include <string>
#include <string_view>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

// Every body a subquery reads - a union, a conditional, a chain of NEXT segments, another
// subquery - parsed inside EXISTS { }, COUNT { }, COLLECT { } and CALL { }
class NestedSubqueryTest : public ExpectedASTTest {
protected:
    // Takes a builder rather than the body, since each parse resets the expected AST
    template <typename BuildBody>
    void expectInEverySubquery(std::string_view body, BuildBody buildBody) {
        const std::string text(body);

        expectParse("RETURN EXISTS { " + text + " }", {query({}, returning({exists(buildBody())}))});

        expectParse("RETURN COUNT { " + text + " }", {query({}, returning({countSubquery(buildBody())}))});

        expectParse("RETURN COLLECT { " + text + " }", {query({}, returning({collectSubquery(buildBody())}))});

        expectParse("CALL { " + text + " }", {query({callSubquery({._body = buildBody()})})});
    }

    SinglePartQuery* returnAs(int64_t value, std::string_view alias) {
        return query({}, returning({named(integer(value), alias)}));
    }
};

TEST_F(NestedSubqueryTest, holdsAUnionOfBracedQueries) {
    expectInEverySubquery("{ RETURN 1 AS a } UNION { RETURN 2 AS a }", [this] {
        return unionQuery(returnAs(1, "a"), {{._query = returnAs(2, "a")}});
    });
}

TEST_F(NestedSubqueryTest, holdsAUnionAll) {
    expectInEverySubquery("RETURN 1 AS a UNION ALL RETURN 2 AS a", [this] {
        return unionQuery(returnAs(1, "a"), {{._query = returnAs(2, "a"), ._all = true}});
    });
}

TEST_F(NestedSubqueryTest, holdsAUnionWhoseOperandIsAUnion) {
    expectInEverySubquery("{ { RETURN 1 AS a } UNION { RETURN 2 AS a } } UNION { RETURN 3 AS a }", [this] {
        return unionQuery(unionQuery(returnAs(1, "a"), {{._query = returnAs(2, "a")}}), {{._query = returnAs(3, "a")}});
    });
}

TEST_F(NestedSubqueryTest, holdsConditionalBranchesInBraces) {
    expectInEverySubquery("WHEN true THEN { RETURN 1 AS a } ELSE { RETURN 2 AS a }", [this] {
        return conditional({{._condition = boolean(true), ._query = returnAs(1, "a")}, {._query = returnAs(2, "a")}});
    });
}

TEST_F(NestedSubqueryTest, holdsAConditionalBranchHoldingAUnion) {
    expectInEverySubquery("WHEN true THEN { RETURN 1 AS a UNION RETURN 2 AS a } ELSE RETURN 3 AS a", [this] {
        return conditional({{._condition = boolean(true),
                             ._query = unionQuery(returnAs(1, "a"), {{._query = returnAs(2, "a")}})},
                            {._query = returnAs(3, "a")}});
    });
}

TEST_F(NestedSubqueryTest, holdsAChainOfSegments) {
    expectInEverySubquery("RETURN 1 AS a NEXT RETURN a", [this] {
        return query({with({._projection = projection({._items = {named(integer(1), "a")}}), ._segmentReturn = true})},
                     returning({symbol("a")}));
    });
}

TEST_F(NestedSubqueryTest, holdsAUnionBeforeASegment) {
    expectInEverySubquery("{ RETURN 1 AS a } UNION { RETURN 2 AS a } NEXT RETURN a", [this] {
        return query({nextSegment(callSubquery({._body = unionQuery(returnAs(1, "a"), {{._query = returnAs(2, "a")}})}))},
                     returning({symbol("a")}));
    });
}

TEST_F(NestedSubqueryTest, holdsAConditionalBeforeASegment) {
    expectInEverySubquery("WHEN true THEN RETURN 1 AS a NEXT RETURN a", [this] {
        return query({nextSegment(callSubquery({._body = conditional({{._condition = boolean(true),
                                                                       ._query = returnAs(1, "a")}})}))},
                     returning({symbol("a")}));
    });
}

TEST_F(NestedSubqueryTest, holdsAnExistsSubquery) {
    expectInEverySubquery("RETURN EXISTS { RETURN 1 } AS a", [this] {
        return query({}, returning({named(exists(query({}, returning({integer(1)}))), "a")}));
    });
}

TEST_F(NestedSubqueryTest, holdsACountSubquery) {
    expectInEverySubquery("RETURN COUNT { RETURN 1 } AS a", [this] {
        return query({}, returning({named(countSubquery(query({}, returning({integer(1)}))), "a")}));
    });
}

TEST_F(NestedSubqueryTest, holdsACollectSubquery) {
    expectInEverySubquery("RETURN COLLECT { RETURN 1 } AS a", [this] {
        return query({}, returning({named(collectSubquery(query({}, returning({integer(1)}))), "a")}));
    });
}

TEST_F(NestedSubqueryTest, holdsACallSubquery) {
    expectInEverySubquery("CALL { RETURN 1 AS b } RETURN b AS a", [this] {
        return query({callSubquery({._body = returnAs(1, "b")})}, returning({named(symbol("b"), "a")}));
    });
}

TEST_F(NestedSubqueryTest, holdsASubqueryInThePredicateOfAPatternBody) {
    expectParse("RETURN EXISTS { (a)-->(b) WHERE COUNT { (b)-->() } > 1 }",
                {query({},
                       returning({exists(query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                                                  edge({._direction = EdgePattern::Direction::Forward}),
                                                                                  node({._symbol = "b"})})},
                                                                           where(binary(BinaryOperator::GreaterThan,
                                                                                        countSubquery(query({match({._pattern = pattern({path({node({._symbol = "b"}),
                                                                                                                                               edge({._direction = EdgePattern::Direction::Forward}),
                                                                                                                                               node({})})})})})),
                                                                                        integer(1))))})}))}))});

    expectParse("RETURN COUNT { (a)-->(b) WHERE EXISTS { (b)-->() } }",
                {query({},
                       returning({countSubquery(query({match({._pattern = pattern({path({node({._symbol = "a"}),
                                                                                         edge({._direction = EdgePattern::Direction::Forward}),
                                                                                         node({._symbol = "b"})})},
                                                                                  where(exists(query({match({._pattern = pattern({path({node({._symbol = "b"}),
                                                                                                                                        edge({._direction = EdgePattern::Direction::Forward}),
                                                                                                                                        node({})})})})}))))})}))}))});
}

TEST_F(NestedSubqueryTest, holdsSubqueriesThreeLevelsDeep) {
    expectParse("RETURN EXISTS { CALL { RETURN COUNT { RETURN COLLECT { RETURN 1 } AS c } AS b } RETURN b }",
                {query({},
                       returning({exists(query({callSubquery({._body = query({},
                                                                             returning({named(countSubquery(query({},
                                                                                                                  returning({named(collectSubquery(query({},
                                                                                                                                                         returning({integer(1)}))),
                                                                                                                                   "c")}))),
                                                                                              "b")}))})},
                                               returning({symbol("b")})))}))});

    expectParse("CALL { CALL { CALL { RETURN 1 AS a } RETURN a } RETURN a }",
                {query({callSubquery({._body = query({callSubquery({._body = query({callSubquery({._body = returnAs(1, "a")})},
                                                                                   returning({symbol("a")}))})},
                                                     returning({symbol("a")}))})})});
}

TEST_F(NestedSubqueryTest, holdsScopedAndOptionalCallsWithNestedBodies) {
    expectParse("MATCH (n) CALL (n) { CALL (n) { RETURN n AS m } RETURN m } RETURN m",
                {query({match({._pattern = pattern({path({node({._symbol = "n"})})})}),
                        callSubquery({._body = query({callSubquery({._body = query({}, returning({named(symbol("n"), "m")})),
                                                                    ._imports = {"n"},
                                                                    ._scope = true})},
                                                     returning({symbol("m")})),
                                      ._imports = {"n"},
                                      ._scope = true})},
                       returning({symbol("m")}))});

    expectParse("OPTIONAL CALL (*) { { RETURN 1 AS a } UNION { RETURN 2 AS a } }",
                {query({callSubquery({._body = unionQuery(returnAs(1, "a"), {{._query = returnAs(2, "a")}}),
                                      ._scope = true,
                                      ._importsEverything = true,
                                      ._optional = true})})});

    expectParse("CALL () { WHEN true THEN { RETURN 1 AS a } }",
                {query({callSubquery({._body = conditional({{._condition = boolean(true), ._query = returnAs(1, "a")}}),
                                      ._scope = true})})});
}

TEST_F(NestedSubqueryTest, holdsAForeachInACall) {
    expectParse("CALL { FOREACH (x IN [1] | CREATE (n)) }",
                {query({callSubquery({._body = query({callSubquery({._body = query({unwind(list({integer(1)}), "x"),
                                                                                    createStmt(pattern({path({node({._symbol = "n"})})}))}),
                                                                    ._scope = true,
                                                                    ._importsEverything = true})})})})});
}
