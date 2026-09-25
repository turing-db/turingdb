#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "versioning/ChangeID.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// REMOVE takes a property off a node or an edge. Afterwards the property reads null, a
// pattern on its old value no longer finds the entity, and a name no property in the
// graph carries is removed without writing anything.
class RemovePropertyTest : public WriteQueryTest {
protected:
    void expectWriteRejected(std::string_view query,
                             QueryStatus::Status stage,
                             std::string_view reason) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), stage) << "query: " << query << "\nerror: " << error;
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }
};

TEST_F(RemovePropertyTest, readsNullForThePropertyRemovedFromAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) REMOVE p.age RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"null"}});
}

TEST_F(RemovePropertyTest, readsNullForTheStringPropertyRemovedFromAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) REMOVE p.dob RETURN p.dob", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.dob", {{"null"}});
}

TEST_F(RemovePropertyTest, readsNullForTheBoolPropertyRemovedFromAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) REMOVE p.isFrench RETURN p.isFrench", {{"null"}});

    expectRows("MATCH (p:Person) WHERE p.isFrench RETURN p.name", {{"Adam"}, {"Maxime"}, {"Luc"}});
}

TEST_F(RemovePropertyTest, readsNullForTheListAndDateTimePropertiesRemoved) {
    applyWrite("CREATE (t:Tag {name: 'x', l: [1, 2], d: datetime('2024-01-01T00:00:00Z')})");

    expectWriteRows("MATCH (t:Tag) REMOVE t.l, t.d RETURN t.l, t.d", {{"null", "null"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.l, t.d", {{"x", "null", "null"}});
}

// Remy and Adam are the two Person nodes carrying an age, both 32
TEST_F(RemovePropertyTest, theRemovedPropertyNoLongerMatchesItsOldValue) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) REMOVE p.age");

    expectRows("MATCH (p:Person {age: 32}) RETURN p.name", {{"Adam"}});
}

// simpledb holds 8 Person nodes, and 6 of them carried no age to begin with
TEST_F(RemovePropertyTest, theRemovedPropertyReadsAsNull) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) REMOVE p.age");

    expectRows("MATCH (p:Person) WHERE p.age IS NULL RETURN p.name",
               {{"Remy"},
                {"Maxime"},
                {"Luc"},
                {"Martina"},
                {"Suhas"},
                {"Cyrus"},
                {"Doruk"}});
}

TEST_F(RemovePropertyTest, removesThePropertyAMatchLookedTheNodeUpBy) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) REMOVE p.name");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.dob", {});
    expectRows("MATCH (p:Person) WHERE p.name IS NULL RETURN p.dob", {{"18/01"}});
}

TEST_F(RemovePropertyTest, removesThePropertyOnEveryRowTheNodeIsMatchedOn) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'})-[:INTERESTED_IN]->(i) "
                    "REMOVE p.age "
                    "RETURN i.name, p.age",
                    {{"Ghosts", "null"}, {"Computers", "null"}, {"Eighties", "null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"null"}});
}

TEST_F(RemovePropertyTest, writesThePropertyAgainAfterRemovingIt) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) REMOVE p.age SET p.age = 50 RETURN p.age", {{"50"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"50"}});
}

TEST_F(RemovePropertyTest, removesThePropertyASetOfTheSameQueryWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Maxime'}) SET p.age = 40 REMOVE p.age RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Maxime'}) RETURN p.age", {{"null"}});
}

// Sorted by name, Remy and Suhas are the last two of the 8 Person nodes
TEST_F(RemovePropertyTest, removesThePropertyOnTheRowsAWithOrderedAndSkipped) {
    expectWriteRows("MATCH (p:Person) WITH p ORDER BY p.name SKIP 6 REMOVE p.isFrench RETURN p.name",
                    {{"Remy"}, {"Suhas"}});

    expectRows("MATCH (p:Person) WHERE p.isFrench IS NULL RETURN p.name", {{"Remy"}, {"Suhas"}});
}

// Remy is the one Person interested in three things
TEST_F(RemovePropertyTest, removesThePropertyOnTheRowsAnAggregatingWithKept) {
    expectWriteRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH p, count(i) AS c WHERE c >= 3 "
                    "REMOVE p.hasPhD "
                    "RETURN p.name, c",
                    {{"Remy", "3"}});

    expectRows("MATCH (p:Person) WHERE p.hasPhD IS NULL RETURN p.name", {{"Remy"}});
}

TEST_F(RemovePropertyTest, removesEveryPropertyTheClauseNames) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) REMOVE p.age, p.dob RETURN p.age, p.dob",
                    {{"null", "null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age, p.dob", {{"null", "null"}});
}

TEST_F(RemovePropertyTest, readsNullForThePropertyRemovedFromAMatchedEdge) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
                    "REMOVE e.duration "
                    "RETURN e.duration",
                    {{"null"}});

    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN e.name, e.duration",
               {{"Remy -> Adam", "null"},
                {"Adam -> Remy", "20"},
                {"Ghosts -> Remy", "200"}});
}

TEST_F(RemovePropertyTest, removesThePropertyFromEveryEdgeAnUndirectedPatternMatched) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]-(b:Person {name: 'Adam'}) "
                    "REMOVE e.duration "
                    "RETURN e.name, e.duration",
                    {{"Remy -> Adam", "null"}, {"Adam -> Remy", "null"}});
}

// A name no property in the graph carries: the clause writes nothing, and it must not
// intern a property type for the name either
TEST_F(RemovePropertyTest, removesAPropertyNoEntityCarries) {
    expectWriteRowCount("MATCH (p:Person) REMOVE p.favouriteColour RETURN p.name", 8);

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.favouriteColour", {{"null"}});
}

// Doruk walks no KNOWS_WELL edge, so f is null on every row the join kept
TEST_F(RemovePropertyTest, removesNoPropertyWhenThePatternMatchedNoRow) {
    applyWrite("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "REMOVE f.dob");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

// f matched Remy and Adam; the six padded rows name no node, so those two alone lose the dob
TEST_F(RemovePropertyTest, removesThePropertyOnTheRowsThePatternMatchedAlone) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) REMOVE f.dob");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {{"Maxime"}, {"Luc"}});
}

// The node is one this change wrote and has not committed, so the removal lands on the
// write buffer's own row rather than as an update to a committed entity
TEST_F(RemovePropertyTest, removesThePropertyAPendingNodeWasCreatedWith) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH t REMOVE t.name RETURN t.name", {{"null"}});
}

TEST_F(RemovePropertyTest, readsNullForThePropertyRemovedInThePartThatCreatedTheNode) {
    expectWriteRows("CREATE (t:Tag {name: 'x', dob: '01/01'}) REMOVE t.name RETURN t.name, t.dob",
                    {{"null", "01/01"}});
}

// The same removal, in the part that created the node rather than behind a WITH
TEST_F(RemovePropertyTest, removesThePropertyInThePartThatCreatedTheNode) {
    applyWrite("CREATE (t:Tag {name: 'x', dob: '01/01'}) REMOVE t.name");

    expectRows("MATCH (t:Tag) RETURN t.name, t.dob", {{"null", "01/01"}});
}

TEST_F(RemovePropertyTest, rejectsTheRemovalOfALabel) {
    expectWriteRejected("MATCH (p:Person {name: 'Remy'}) REMOVE p:Founder",
                        QueryStatus::Status::PARSE_ERROR,
                        "Not implemented");
}

TEST_F(RemovePropertyTest, rejectsTheRemovalOfAPropertyOfANonEntityVariable) {
    expectWriteRejected("WITH 1 AS x REMOVE x.age",
                        QueryStatus::Status::ANALYZE_ERROR,
                        "must be a node or edge");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
