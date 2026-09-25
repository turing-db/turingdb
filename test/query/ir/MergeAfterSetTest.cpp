#include <gtest/gtest.h>

#include <string_view>

#include "QueryStatus.h"

#include "versioning/ChangeID.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE behind a SET of a property its pattern names, on a node or an edge the graph
// already holds. The graph keeps the value from before the change until the commit, and
// the MERGE has to match the entity by the value the change gave it.
//
// Of simpledb's 8 Person nodes, Remy and Adam carry an age of 32. Its three KNOWS_WELL
// edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy.
class MergeAfterSetTest : public WriteQueryTest {
protected:
    void stageWrite(std::string_view query, const ChangeID& changeID) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(MergeAfterSetTest, matchesTheNodeByTheValueTheSetWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.name = 'Remi' "
                    "WITH count(*) AS renamed MERGE (q:Person {name: 'Remi'}) RETURN q.name",
                    {{"Remi"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"8"}});
}

TEST_F(MergeAfterSetTest, createsANodeForTheValueTheSetOverwrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.name = 'Remi' "
                    "WITH count(*) AS renamed MERGE (q:Person {name: 'Remy'}) RETURN q.name",
                    {{"Remy"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"9"}});
}

TEST_F(MergeAfterSetTest, doesNotMatchTheValueASetToNullTookOff) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null "
                    "WITH count(*) AS cleared MERGE (q:Person {age: 32}) RETURN q.name",
                    {{"Adam"}});
}

TEST_F(MergeAfterSetTest, matchesTheEdgeByTheValueTheSetWrote) {
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e.duration = 21 "
                    "WITH count(*) AS updated "
                    "MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "MERGE (a)-[f:KNOWS_WELL {duration: 21}]->(b) RETURN f.name",
                    {{"Remy -> Adam"}});

    expectRows("MATCH ()-[f:KNOWS_WELL]->() RETURN count(f)", {{"3"}});
}

TEST_F(MergeAfterSetTest, matchesTheNodeByTheValueAnEarlierQueryOfTheChangeWrote) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MATCH (p:Person {name: 'Remy'}) SET p.name = 'Remi'", changeID);
    stageWrite("MERGE (q:Person {name: 'Remi'})", changeID);
    submit(changeID);

    expectRows("MATCH (q:Person) RETURN count(q)", {{"8"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
