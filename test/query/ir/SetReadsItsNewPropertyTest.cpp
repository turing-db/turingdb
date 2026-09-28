#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class SetReadsItsNewPropertyTest : public WriteQueryTest {
};

TEST_F(SetReadsItsNewPropertyTest, setsANewPropertyFromItsOwnMissingValue) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.visits = coalesce(p.visits, 0) + 1 RETURN p.visits",
                    {{"1"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.visits", {{"1"}});
}

TEST_F(SetReadsItsNewPropertyTest, setsANewEdgePropertyFromItsOwnMissingValue) {
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
                    "SET e.tag = coalesce(e.tag, 'none') "
                    "RETURN e.tag",
                    {{"none"}});
}

TEST_F(SetReadsItsNewPropertyTest, mergesOnMatchFromTheNewPropertyItsSetWrites) {
    expectWriteRows("MERGE (p:Person {name: 'Adam'}) ON MATCH SET p.visits = coalesce(p.visits, 0) + 1 RETURN p.visits",
                    {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
