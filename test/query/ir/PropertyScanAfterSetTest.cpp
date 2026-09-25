#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MATCH on a property value, behind a SET of that property on a node the graph already
// holds. The graph keeps the value from before the change until the commit, and the MATCH
// has to find the node by the value the change gave it.
//
// Of simpledb's 8 Person nodes, Remy and Adam carry an age of 32, and Remy, Adam, Maxime
// and Luc are French.
class PropertyScanAfterSetTest : public WriteQueryTest {
protected:
    void stageWrite(std::string_view query, const ChangeID& changeID) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectChangeRows(std::string_view query, const ChangeID& changeID, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }
};

TEST_F(PropertyScanAfterSetTest, matchesTheValueTheSetWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 33 WITH p MATCH (q:Person {age: 33}) RETURN q.name",
                    {{"Remy"}});
}

TEST_F(PropertyScanAfterSetTest, doesNotMatchTheValueTheSetOverwrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 33 WITH p MATCH (q:Person {age: 32}) RETURN q.name",
                    {{"Adam"}});
}

TEST_F(PropertyScanAfterSetTest, doesNotMatchTheValueASetToNullTookOff) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null WITH p MATCH (q:Person {age: 32}) RETURN q.name",
                    {{"Adam"}});
}

TEST_F(PropertyScanAfterSetTest, doesNotMatchTheValueARemoveTookOff) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) REMOVE p.age WITH p MATCH (q:Person {age: 32}) RETURN q.name",
                    {{"Adam"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheNodeOnceWhenTheSetWroteTheValueItHeld) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 32 WITH p MATCH (q:Person {age: 32}) RETURN q.name",
                    {{"Remy"}, {"Adam"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheValueOfTheLastSet) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 33 WITH p SET p.age = 34 "
                    "WITH p MATCH (q:Person {age: 34}) RETURN q.name",
                    {{"Remy"}});
}

TEST_F(PropertyScanAfterSetTest, doesNotMatchTheValueALaterSetOverwrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 33 WITH p SET p.age = 34 "
                    "WITH p MATCH (q:Person {age: 33}) RETURN q.name",
                    {});
}

TEST_F(PropertyScanAfterSetTest, matchesTheValueTheSetWroteInAWhere) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 33 WITH p MATCH (q:Person) WHERE q.age = 33 RETURN q.name",
                    {{"Remy"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheValueTheSetWroteWithoutALabel) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 33 WITH p MATCH (q {age: 33}) RETURN q.name",
                    {{"Remy"}});
}

// Ghosts is an Interest, so the Person scan leaves it out and the unlabelled one finds it
TEST_F(PropertyScanAfterSetTest, matchesTheValueOnANodeOfTheScannedLabelAlone) {
    expectWriteRows("MATCH (i:Interest {name: 'Ghosts'}) SET i.age = 32 WITH i MATCH (q:Person {age: 32}) RETURN q.name",
                    {{"Remy"}, {"Adam"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheValueOnANodeOfAnyLabelWithoutALabel) {
    expectWriteRows("MATCH (i:Interest {name: 'Ghosts'}) SET i.age = 32 WITH i MATCH (q {age: 32}) RETURN q.name",
                    {{"Remy"}, {"Adam"}, {"Ghosts"}});
}

TEST_F(PropertyScanAfterSetTest, matchesAPropertyTheGraphDoesNotHold) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.colour = 'red' "
                    "WITH p MATCH (q:Person {colour: 'red'}) RETURN q.name",
                    {{"Remy"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheStringTheSetWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.name = 'Remi' "
                    "WITH p MATCH (q:Person {name: 'Remi'}) RETURN q.name",
                    {{"Remi"}});
}

TEST_F(PropertyScanAfterSetTest, doesNotMatchTheStringTheSetOverwrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.name = 'Remi' "
                    "WITH p MATCH (q:Person {name: 'Remy'}) RETURN q.name",
                    {});
}

TEST_F(PropertyScanAfterSetTest, matchesTheBoolTheSetWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.isFrench = false "
                    "WITH p MATCH (q:Person {isFrench: true}) RETURN q.name",
                    {{"Adam"}, {"Maxime"}, {"Luc"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheValueACreateSetWroteOnAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CREATE (t:Tag) SET p.age = 33 "
                    "WITH t MATCH (q:Person {age: 33}) RETURN q.name",
                    {{"Remy"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheValueAMergeSetOnMatch) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) ON MATCH SET p.age = 33 "
                    "WITH count(*) AS merged MATCH (q:Person {age: 33}) RETURN q.name",
                    {{"Remy"}});
}

TEST_F(PropertyScanAfterSetTest, setsEveryNodeItsOwnScanFound) {
    expectWriteRows("MATCH (p:Person {age: 32}) SET p.age = 33 RETURN p.name", {{"Remy"}, {"Adam"}});

    expectRows("MATCH (q:Person {age: 33}) RETURN q.name", {{"Remy"}, {"Adam"}});
}

TEST_F(PropertyScanAfterSetTest, matchesTheValueAnEarlierQueryOfTheChangeWrote) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = 33", changeID);

    expectChangeRows("MATCH (q:Person {age: 33}) RETURN q.name", changeID, {{"Remy"}});
    expectChangeRows("MATCH (q:Person {age: 32}) RETURN q.name", changeID, {{"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
