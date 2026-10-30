#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A constant key groups the rows a CREATE or a MERGE writes into one group, and gets no
// group when the clause runs over no row. Remy is the one person of simpledb so named.
class ConstantKeyWriteTest : public WriteQueryTest {
};

TEST_F(ConstantKeyWriteTest, groupsTheCreatedNodesUnderAConstantKey) {
    expectWriteRows("MATCH (n:Person) CREATE (m:Tmp {v: 1}) RETURN 1 AS k, count(m)", {{"1", "8"}});
    expectWriteRows("MATCH (n:Person {name: 'Nobody'}) CREATE (m:Tmp {v: 2}) RETURN 1 AS k, count(m)", {});
    expectWriteRows("CREATE (m:Tmp {v: 3}) RETURN 1 AS k, count(m)", {{"1", "1"}});

    expectRows("MATCH (t:Tmp) RETURN 1 AS k, t.v AS v, count(t) ORDER BY v", {{"1", "1", "8"}, {"1", "3", "1"}});
    expectRows("MATCH (t:Tmp) WHERE t.v = 2 RETURN 1 AS k, count(t)", {});
}

TEST_F(ConstantKeyWriteTest, groupsTheMergedNodesUnderAConstantKey) {
    expectWriteRows("MERGE (x:Person {name: 'Remy'}) RETURN 'k' AS k, count(x)", {{"k", "1"}});
    expectWriteRows("MERGE (x:Tmp {v: 4}) RETURN 'k' AS k, count(x), x.v", {{"k", "1", "4"}});
    expectWriteRows("MATCH (n:Person) MERGE (x:Tmp {v: 5}) RETURN 'k' AS k, x.v AS v, count(*)", {{"k", "5", "8"}});

    expectRows("MATCH (t:Tmp) RETURN 1 AS k, t.v AS v, count(t) ORDER BY v", {{"1", "4", "1"}, {"1", "5", "1"}});
}
