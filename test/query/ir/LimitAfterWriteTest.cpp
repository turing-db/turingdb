#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A LIMIT keeps the rows it passes on and drops the rest, and the writes before it apply
// to every row all the same. 70000 unwound rows span two chunks of 65536.
class LimitAfterWriteTest : public WriteQueryTest {
};

TEST_F(LimitAfterWriteTest, createsForEveryRowALaterLimitDrops) {
    expectWriteRows("UNWIND range(1, 70000) AS i CREATE (:W {a: i}) RETURN i LIMIT 1", {{"1"}});

    expectRows("MATCH (w:W) RETURN count(w)", {{"70000"}});
}

TEST_F(LimitAfterWriteTest, createsUnderALimitOfZero) {
    expectWriteRows("UNWIND range(1, 10) AS i CREATE (:W {a: i}) RETURN i LIMIT 0", {});

    expectRows("MATCH (w:W) RETURN count(w)", {{"10"}});
}

TEST_F(LimitAfterWriteTest, setsForEveryRowBeforeTheLimitOfAWith) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) UNWIND range(1, 70000) AS i SET n.age = i WITH n LIMIT 1 RETURN n.age",
                    {{"70000"}});
}

TEST_F(LimitAfterWriteTest, createsForEveryRowBeforeASkipAndALimit) {
    applyWrite("UNWIND range(1, 70000) AS i CREATE (:W {a: i}) WITH i SKIP 1 LIMIT 1 CREATE (:V {b: i})");

    expectRows("MATCH (w:W) RETURN count(w)", {{"70000"}});
    expectRows("MATCH (v:V) RETURN v.b", {{"2"}});
}

TEST_F(LimitAfterWriteTest, stopsTheScanOfALimitBeforeAWrite) {
    applyWrite("MATCH (n:Person) WITH n ORDER BY n.name LIMIT 1 SET n.first = true");

    expectRows("MATCH (n:Person) WHERE n.first RETURN n.name", {{"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
