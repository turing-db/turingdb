#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CALL body runs once per incoming row, and each run reads what the runs before it wrote.
// Cyrus, Suhas and Doruk are interested in Gym in simpledb.
class CallSubqueryReadsItsWritesTest : public WriteQueryTest {
};

TEST_F(CallSubqueryReadsItsWritesTest, returnsWhatEachRunWrote) {
    applyWrite("CREATE (:Counter {count: 0})");

    expectWriteRows("UNWIND [1, 2, 3] AS x CALL () { MATCH (n:Counter) SET n.count = n.count + 1 RETURN n.count AS c } RETURN x, c",
                    {{"1", "1"}, {"2", "2"}, {"3", "3"}});
}

TEST_F(CallSubqueryReadsItsWritesTest, readsWhatTheRunBeforeWroteThroughAWith) {
    applyWrite("CREATE (:Counter {count: 0})");
    applyWrite("UNWIND [1, 2, 3] AS x CALL (x) { MATCH (n:Counter) WITH n, n.count AS before SET n.count = before + 1 }");

    expectRows("MATCH (n:Counter) RETURN n.count", {{"3"}});
}

TEST_F(CallSubqueryReadsItsWritesTest, countsOneRunPerOuterRow) {
    expectWriteRows("MATCH (p:Person) CALL (p) { MATCH (g:Interest {name: 'Gym'}) SET g.visits = coalesce(g.visits, 0) + 1 RETURN g.visits AS v } RETURN v",
                    {{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
