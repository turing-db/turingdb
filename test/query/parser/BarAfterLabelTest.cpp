#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "ExpectedASTTest.h"
#include "ParseOutcome.h"

using namespace db;
using namespace turing::test;

class BarAfterLabelTest : public ExpectedASTTest {
protected:
    static void expectParses(std::string_view query) {
        std::string report;
        renderParseError(query, report);

        EXPECT_EQ(report, "") << query;
    }
};

TEST_F(BarAfterLabelTest, readsTheBarBeforeACaseInAComprehension) {
    expectParse("MATCH (y) RETURN [x IN [1] WHERE x:A | CASE WHEN y:B|C THEN 1 END]",
                {query({match({._pattern = pattern({path({node({._symbol = "y"})})})})},
                       returning({listComprehension("x",
                                                    list({integer(1)}),
                                                    entityType("x", labels({"A"})),
                                                    caseExpr(nullptr,
                                                             {{._tests = {{._value = entityType("y",
                                                                                                labelOr({labels({"B"}),
                                                                                                         labels({"C"})}))}},
                                                               ._then = integer(1)}}))}))});
}

TEST_F(BarAfterLabelTest, readsTheBarBeforeACaseInAReduce) {
    expectParse("MATCH (y) RETURN reduce(s = 0, x IN y:A | CASE WHEN y:B|C THEN 1 END)",
                {query({match({._pattern = pattern({path({node({._symbol = "y"})})})})},
                       returning({reduce("s",
                                         integer(0),
                                         "x",
                                         entityType("y", labels({"A"})),
                                         caseExpr(nullptr,
                                                  {{._tests = {{._value = entityType("y",
                                                                                     labelOr({labels({"B"}),
                                                                                              labels({"C"})}))}},
                                                    ._then = integer(1)}}))}))});
}

TEST_F(BarAfterLabelTest, readsTheBarBeforeAnUpdateInAForeach) {
    expectParses("MATCH (y) FOREACH (x IN y:A | SET y.f = y:B|C)");
    expectParses("MATCH (y) FOREACH (x IN y:A | CREATE (z))");
    expectParses("MATCH (y) FOREACH (x IN y:A | DETACH DELETE y)");
}

TEST_F(BarAfterLabelTest, readsTheBarWhenTheOtherBarIsInsideACase) {
    expectParse("MATCH (x) RETURN [y IN [x] WHERE y:A|B AND CASE WHEN x:C|D THEN true END]",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})})})},
                       returning({listComprehension("y",
                                                    list({symbol("x")}),
                                                    entityType("y", labels({"A"})),
                                                    logical(LogicalOperator::And,
                                                            {symbol("B"),
                                                             caseExpr(nullptr,
                                                                      {{._tests = {{._value = entityType("x",
                                                                                                         labelOr({labels({"C"}),
                                                                                                                  labels({"D"})}))}},
                                                                        ._then = boolean(true)}})}))}))});

    expectParse("MATCH (x) RETURN reduce(s = 0, y IN x:A | s + CASE WHEN x:B|C THEN 1 ELSE 0 END)",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})})})},
                       returning({reduce("s",
                                         integer(0),
                                         "y",
                                         entityType("x", labels({"A"})),
                                         binary(BinaryOperator::Add,
                                                symbol("s"),
                                                caseExpr(nullptr,
                                                         {{._tests = {{._value = entityType("x",
                                                                                            labelOr({labels({"B"}),
                                                                                                     labels({"C"})}))}},
                                                           ._then = integer(1)}},
                                                         integer(0))))}))});
}

TEST_F(BarAfterLabelTest, readsAKeywordLabelBeforeTheBarAsADisjunction) {
    expectParse("MATCH (x) RETURN [y IN [x] WHERE y:A|SET | y]",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})})})},
                       returning({listComprehension("y",
                                                    list({symbol("x")}),
                                                    entityType("y", labelOr({labels({"A"}), labels({"SET"})})),
                                                    symbol("y"))}))});

    expectParses("MATCH (x) FOREACH (y IN x:A|SET | SET x.f = 1)");
}

TEST_F(BarAfterLabelTest, readsACaseNamingAKeyBeforeTheBarAsAName) {
    expectParse("MATCH (x) RETURN [y IN [x] WHERE y:A|B AND y.case - 1 > 0 | y]",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})})})},
                       returning({listComprehension("y",
                                                    list({symbol("x")}),
                                                    logical(LogicalOperator::And,
                                                            {entityType("y", labelOr({labels({"A"}), labels({"B"})})),
                                                             binary(BinaryOperator::GreaterThan,
                                                                    binary(BinaryOperator::Sub, property({"y", "case"}), integer(1)),
                                                                    integer(0))}),
                                                    symbol("y"))}))});

    expectParse("MATCH (x) RETURN [y IN [x] WHERE y:A|B AND $case + 1 > 0 | y]",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})})})},
                       returning({listComprehension("y",
                                                    list({symbol("x")}),
                                                    logical(LogicalOperator::And,
                                                            {entityType("y", labelOr({labels({"A"}), labels({"B"})})),
                                                             binary(BinaryOperator::GreaterThan,
                                                                    binary(BinaryOperator::Add, parameter("case"), integer(1)),
                                                                    integer(0))}),
                                                    symbol("y"))}))});

    expectParse("MATCH (x) RETURN [y IN [x] WHERE y:A|B AND y.case[0] > 0 | y]",
                {query({match({._pattern = pattern({path({node({._symbol = "x"})})})})},
                       returning({listComprehension("y",
                                                    list({symbol("x")}),
                                                    logical(LogicalOperator::And,
                                                            {entityType("y", labelOr({labels({"A"}), labels({"B"})})),
                                                             binary(BinaryOperator::GreaterThan,
                                                                    index(property({"y", "case"}), integer(0)),
                                                                    integer(0))}),
                                                    symbol("y"))}))});
}
