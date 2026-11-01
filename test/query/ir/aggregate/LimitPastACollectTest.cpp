#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A LIMIT after a collect bounds the rows the collect emits, and the collect still reads every
// row it gathers. The 8 people have 15 interests between them.
class LimitPastACollectTest : public WriteQueryTest {
};

TEST_F(LimitPastACollectTest, collectsEveryRowBeforeALimitOfOne) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-->(m:Interest) RETURN size(collect(m.name)) AS s LIMIT 1", {{"15"}});
}

TEST_F(LimitPastACollectTest, countsAndCollectsEveryRowBeforeALimitOfOne) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-->(m:Interest) RETURN count(m) AS k, size(collect(m.name)) AS s LIMIT 1",
               {{"15", "15"}});
}

TEST_F(LimitPastACollectTest, collectsEveryRowOfEachGroupALimitKeepsOfMore) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-->(m:Interest) WITH n, size(collect(m.name)) AS s LIMIT 3 "
               "MATCH (n)-->(i:Interest) WITH n, s, count(i) AS k RETURN s = k",
               {{"true"}, {"true"}, {"true"}});
}

TEST_F(LimitPastACollectTest, collectsEveryRowOfTheGroupsALimitKeeps) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-->(m:Interest) WITH n.name AS name, collect(m.name) AS c LIMIT 8 "
               "RETURN name, size(c)",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}
