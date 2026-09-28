#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Remy and Adam are the only nodes with an age in simpledb, both 32
class ScanWrittenValueAfterDeleteTest : public WriteQueryTest {
};

TEST_F(ScanWrittenValueAfterDeleteTest, findsNoDeletedNodeByTheValueTheQuerySetOnIt) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n.age = 1 DETACH DELETE n "
                    "WITH 1 AS x MATCH (m {age: 1}) RETURN m.name",
                    {});
}

TEST_F(ScanWrittenValueAfterDeleteTest, findsNoDeletedNodeByTheValueTheQuerySetOnItUnderALabel) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n.name = 'R2' DETACH DELETE n "
                    "WITH 1 AS x MATCH (m:Person {name: 'R2'}) RETURN m.name",
                    {});
}

TEST_F(ScanWrittenValueAfterDeleteTest, findsTheNodeTheQuerySetAndKept) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}), (a:Person {name: 'Adam'}) SET n.age = 1, a.age = 1 DETACH DELETE n "
                    "WITH 1 AS x MATCH (m {age: 1}) RETURN m.name",
                    {{"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
