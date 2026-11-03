#include <gtest/gtest.h>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

// Where a bar may close the expression, a `|` after a label is the bar unless another `|`
// follows at the same depth, as in Neo4j: [x IN l WHERE x:A|B] projects B
class LabelDisjunctionOrBarTest : public ExpectedASTTest {
};

TEST_F(LabelDisjunctionOrBarTest, readsAPipeAsTheBarWhenNoOtherPipeFollows) {
    expectParse("RETURN [x IN xs WHERE x:A | x.name]",
                {query({},
                       returning({listComprehension("x",
                                                    symbol("xs"),
                                                    entityType("x", labels({"A"})),
                                                    property({"x", "name"}))}))});

    expectParse("MATCH (n:Person) RETURN [x IN [n] WHERE x:Founder|Sales]",
                {query({match({._pattern = pattern({path({node({._symbol = "n", ._labels = labels({"Person"})})})})})},
                       returning({listComprehension("x",
                                                    list({symbol("n")}),
                                                    entityType("x", labels({"Founder"})),
                                                    symbol("Sales"))}))});

    expectParse("RETURN [(a)-->(b) WHERE b:A | b]",
                {query({},
                       returning({patternComprehension(match({._pattern = pattern({path({node({._symbol = "a"}),
                                                                                         edge({._direction = EdgePattern::Direction::Forward}),
                                                                                         node({._symbol = "b"})})},
                                                                                  where(entityType("b", labels({"A"}))))}),
                                                       symbol("b"))}))});

    expectParse("RETURN reduce(s = 0, x IN n:A | s)",
                {query({},
                       returning({reduce("s", integer(0), "x", entityType("n", labels({"A"})), symbol("s"))}))});
}

TEST_F(LabelDisjunctionOrBarTest, readsAPipeAsALabelDisjunctionWhenTheBarFollows) {
    expectParse("MATCH (a:Person) RETURN [(a)-[:INTERESTED_IN]->(b) WHERE b:Exotic|Supernatural | b.name]",
                {query({match({._pattern = pattern({path({node({._symbol = "a", ._labels = labels({"Person"})})})})})},
                       returning({patternComprehension(match({._pattern = pattern({path({node({._symbol = "a"}),
                                                                                         edge({._direction = EdgePattern::Direction::Forward,
                                                                                               ._labels = labels({"INTERESTED_IN"})}),
                                                                                         node({._symbol = "b"})})},
                                                                                  where(entityType("b",
                                                                                                   labelOr({labels({"Exotic"}),
                                                                                                            labels({"Supernatural"})}))))}),
                                                       property({"b", "name"}))}))});

    expectParse("RETURN [x IN xs WHERE x:A|B | x]",
                {query({},
                       returning({listComprehension("x",
                                                    symbol("xs"),
                                                    entityType("x", labelOr({labels({"A"}), labels({"B"})})),
                                                    symbol("x"))}))});

    expectParse("RETURN [x IN xs WHERE x:A|B|C | x.name]",
                {query({},
                       returning({listComprehension("x",
                                                    symbol("xs"),
                                                    entityType("x", labelOr({labels({"A"}), labels({"B"}), labels({"C"})})),
                                                    property({"x", "name"}))}))});

    expectParse("RETURN [x IN xs WHERE x:A|B AND x:C | x]",
                {query({},
                       returning({listComprehension("x",
                                                    symbol("xs"),
                                                    logical(LogicalOperator::And,
                                                            {entityType("x", labelOr({labels({"A"}), labels({"B"})})),
                                                             entityType("x", labels({"C"}))}),
                                                    symbol("x"))}))});

    expectParse("RETURN [x IN n:A|B | x]",
                {query({},
                       returning({listComprehension("x",
                                                    entityType("n", labelOr({labels({"A"}), labels({"B"})})),
                                                    nullptr,
                                                    symbol("x"))}))});

    expectParse("RETURN reduce(s = 0, x IN n:A|B | s)",
                {query({},
                       returning({reduce("s",
                                         integer(0),
                                         "x",
                                         entityType("n", labelOr({labels({"A"}), labels({"B"})})),
                                         symbol("s"))}))});

    expectParse("RETURN allReduce(s = 0, x IN n:A|B | s, s > 0)",
                {query({},
                       returning({allReduce("s",
                                            integer(0),
                                            "x",
                                            entityType("n", labelOr({labels({"A"}), labels({"B"})})),
                                            symbol("s"),
                                            binary(BinaryOperator::GreaterThan, symbol("s"), integer(0)))}))});

    expectParse("FOREACH (x IN n:A|B | SET x.y = 1)",
                {query({callSubquery({._body = query({unwind(entityType("n", labelOr({labels({"A"}), labels({"B"})})), "x"),
                                                      set({setProperty(property({"x", "y"}), integer(1))})}),
                                      ._scope = true,
                                      ._importsEverything = true})})});
}

TEST_F(LabelDisjunctionOrBarTest, looksOnlyAtPipesOfItsOwnDepth) {
    expectParse("RETURN [x IN xs WHERE x:A | [y IN ys | y]]",
                {query({},
                       returning({listComprehension("x",
                                                    symbol("xs"),
                                                    entityType("x", labels({"A"})),
                                                    listComprehension("y", symbol("ys"), nullptr, symbol("y")))}))});

    expectParse("RETURN [x IN xs WHERE x:(A|B) | x]",
                {query({},
                       returning({listComprehension("x",
                                                    symbol("xs"),
                                                    entityType("x", labelOr({labels({"A"}), labels({"B"})})),
                                                    symbol("x"))}))});

    expectParse("RETURN [x IN xs WHERE x.a = 1 | x:A|B]",
                {query({},
                       returning({listComprehension("x",
                                                    symbol("xs"),
                                                    binary(BinaryOperator::Equal, property({"x", "a"}), integer(1)),
                                                    entityType("x", labelOr({labels({"A"}), labels({"B"})})))}))});
}
