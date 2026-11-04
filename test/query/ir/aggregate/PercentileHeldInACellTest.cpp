#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class PercentileHeldInACellTest : public WriteQueryTest {
};

TEST_F(PercentileHeldInACellTest, takesThePercentileAnAverageOfCellsAnswers) {
    expectRows("UNWIND [0, 0.25] AS p WITH avg(p) AS q MATCH ()-[e:INTERESTED_IN]->() "
               "RETURN q, percentileCont(e.duration, q), percentileDisc(e.duration, q)",
               {{"0.125000", "12.500000", "10"}});
}

TEST_F(PercentileHeldInACellTest, takesThePercentileASumOfCellsAnswers) {
    expectRows("UNWIND [0, 0.125] AS p WITH sum(p) AS q MATCH ()-[e:INTERESTED_IN]->() "
               "RETURN q, percentileCont(e.duration, q), percentileDisc(e.duration, q)",
               {{"0.125000", "12.500000", "10"}});
}

TEST_F(PercentileHeldInACellTest, takesThePercentileAMapValueHolds) {
    expectRows("UNWIND [{q: 0.125}, {q: 0.5}] AS m MATCH (:Person {name: 'Luc'})-[e:INTERESTED_IN]->() "
               "RETURN m.q, percentileCont(e.duration, m.q)",
               {{"0.125000", "15.625000"}, {"0.500000", "17.500000"}});
}

TEST_F(PercentileHeldInACellTest, rejectsACellHoldingNoNumber) {
    expectError("UNWIND [{q: 'a'}] AS m MATCH ()-[e:INTERESTED_IN]->() RETURN percentileCont(e.duration, m.q)",
                "percentileCont() and percentileDisc() take a percentile between 0.0 and 1.0, not 'String'");
}
