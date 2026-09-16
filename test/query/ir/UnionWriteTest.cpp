#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A union whose branches write. Each branch is a query body of its own, so what one writes
// neither constrains what the next may write nor reaches the rows the next matches.
class UnionWriteTest : public WriteQueryTest {
};

// The write clauses a branch may use are decided by that branch alone: a CREATE in the
// first does not turn the second's SET into the CREATE ... SET the engine has yet to
// support.
TEST_F(UnionWriteTest, letsOneBranchCreateAndAnotherSet) {
    expectWriteRows("CREATE (n:Recruit {name: 'Nina'}) RETURN n.name AS name UNION ALL "
                    "MATCH (m:Founder) SET m.dob = '01/01' RETURN m.name AS name",
                    Rows {{"Nina"}, {"Remy"}, {"Adam"}});

    expectRows("MATCH (n:Recruit) RETURN n.name", Rows {{"Nina"}});
    expectRows("MATCH (n:Founder) RETURN n.dob", Rows {{"01/01"}, {"01/01"}});
}

TEST_F(UnionWriteTest, createsInEveryBranch) {
    expectWriteRows("CREATE (n:Recruit {name: 'Nina'}) RETURN n.name AS name UNION ALL "
                    "CREATE (m:Recruit {name: 'Omar'}) RETURN m.name AS name",
                    Rows {{"Nina"}, {"Omar"}});

    expectRows("MATCH (n:Recruit) RETURN n.name", Rows {{"Nina"}, {"Omar"}});
}

// The branches run in the order they are written, so a branch reads the graph as the
// branches before it left it: the second matches the node the first created, and the
// first - which ran before that write - matches nothing. This is the visibility a MATCH
// behind a CREATE already has in one statement, and the union inherits it.
TEST_F(UnionWriteTest, readsWhatAnEarlierBranchWrote) {
    expectWriteRows("CREATE (n:Recruit {name: 'Nina'}) RETURN n.name AS name UNION ALL "
                    "MATCH (m:Recruit) RETURN m.name AS name",
                    Rows {{"Nina"}, {"Nina"}});
}

TEST_F(UnionWriteTest, doesNotReadWhatALaterBranchWrites) {
    expectWriteRows("MATCH (m:Recruit) RETURN m.name AS name UNION ALL "
                    "CREATE (n:Recruit {name: 'Nina'}) RETURN n.name AS name",
                    Rows {{"Nina"}});
}
