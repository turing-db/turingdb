#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class NullComparisonOperandTest : public WriteQueryTest {
};

TEST_F(NullComparisonOperandTest, comparesNullAgainstAnOrderingAcrossTypes) {
    expectRows("RETURN true = (1 < 'a'), true <> (1 < 'a'), (1 < 'a') = (2 < 'b')",
               {{"null", "null", "null"}});
}

TEST_F(NullComparisonOperandTest, comparesNullAgainstAnOrderingOfAPropertyAcrossTypes) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.hasPhD = (n.name < 5), n.hasPhD <> (n.name < 5)",
               {{"null", "null"}});
}

TEST_F(NullComparisonOperandTest, filtersOutAnInequalityAgainstAnOrderingAcrossTypes) {
    expectRows("MATCH (n:Person) WHERE n.hasPhD <> (n.name < 5) RETURN n", {});
}

TEST_F(NullComparisonOperandTest, comparesACellAgainstAnOrderingAcrossTypes) {
    expectRows("UNWIND [true] AS x RETURN x = (1 < 'a'), x < (1 < 'a')", {{"null", "null"}});
}

TEST_F(NullComparisonOperandTest, comparesAMapValueAgainstAnOrderingAcrossTypes) {
    expectRows("UNWIND [{k: null}, {k: true}] AS m RETURN m.k = (1 < 'a')", {{"null"}, {"null"}});
}

TEST_F(NullComparisonOperandTest, ordersAnOrderingAcrossTypes) {
    expectRows("RETURN (1 < 'a') < true, true >= (1 < 'a')", {{"null", "null"}});
}

TEST_F(NullComparisonOperandTest, coalescesPastAnOrderingAcrossTypes) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN coalesce(n.name < 5, true)", {{"true"}});
}

TEST_F(NullComparisonOperandTest, comparesNullAgainstAComparisonWithNull) {
    expectRows("RETURN true = (1 < null), true = (1 = null), true <> (1 <> null)",
               {{"null", "null", "null"}});
}

TEST_F(NullComparisonOperandTest, coalescesPastAComparisonWithNull) {
    expectRows("RETURN coalesce(1 < null, 1 = null, true)", {{"true"}});
}

TEST_F(NullComparisonOperandTest, comparesNullAgainstASearchOfNull) {
    expectRows("RETURN true = (1 IN null), coalesce(1 IN null, true)", {{"null", "true"}});
}
