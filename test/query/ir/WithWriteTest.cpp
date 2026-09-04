#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The writes a query part after a WITH performs: one per row the barrier published, over
// the entities it published rather than the ones the match produced.
class WithWriteTest : public WriteQueryTest {
protected:
    // A write the engine turns away has to name what is wrong with the query, rather than
    // trip an assertion on the way down
    void expectWriteRejected(std::string_view query, db::QueryStatus::Status stage) {
        db::ChangeID changeID;
        openChange(changeID);

        const db::QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), stage)
            << "query: " << query
            << "\nstage: " << db::QueryStatusDescription::value(status.getStatus())
            << "\nerror: " << error;

        EXPECT_EQ(error.find("Unexpected exception"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }

};

// The SET runs over the one row the cut left, which is Adam: the first Person in name order
TEST_F(WithWriteTest, setsAPropertyOnTheRowsACutLeft) {
    applyWrite("MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 SET p.dob = '02/02'");

    expectRows("MATCH (p:Person) WITH p.name AS name, p.dob AS dob WHERE dob = '02/02' "
               "RETURN name",
               {{"Adam"}});
}

// Gym reaches the barrier three times and leaves it once, so the SET writes it once.
// Eighties was already unreal
TEST_F(WithWriteTest, setsAPropertyOnDedupedRows) {
    applyWrite("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH DISTINCT i WHERE i.name = 'Gym' "
               "SET i.isReal = false");

    expectRows("MATCH (i:Interest {isReal: false}) RETURN i.name",
               {{"Eighties"}, {"Gym"}});
}

// Only the interest more than two Persons reach survives the barrier's filter
TEST_F(WithWriteTest, setsAPropertyOnAGroupTheFilterKept) {
    applyWrite("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH i, count(p) AS fans WHERE fans > 2 "
               "SET i.isReal = false");

    expectRows("MATCH (i:Interest {isReal: false}) RETURN i.name",
               {{"Eighties"}, {"Gym"}});
}

// The barrier published the edge, and the DELETE reads that column: Adam stops knowing
// Remy well, and the edge the other way stays
TEST_F(WithWriteTest, deletesAnEdgeTheBarrierPublished) {
    applyWrite("MATCH (a:Person {name: 'Adam'})-[e:KNOWS_WELL]->(b) WITH e DELETE e");

    expectRows("MATCH (a:Person)-[e:KNOWS_WELL]->(b) RETURN e.name", {{"Remy -> Adam"}});
}

TEST_F(WithWriteTest, detachDeletesANodeTheBarrierPublished) {
    applyWrite("MATCH (p:Person {name: 'Martina'}) WITH p DETACH DELETE p");

    expectCounts("MATCH (p:Person) RETURN count(*)", {7});
}

// Martina is interested in Cooking, so deleting her node alone would leave that edge
// dangling
TEST_F(WithWriteTest, rejectsDeletingAConnectedNodeTheBarrierPublished) {
    expectWriteRejected("MATCH (p:Person {name: 'Martina'}) WITH p DELETE p",
                        QueryStatus::Status::EXEC_ERROR);
}

// One node per row the barrier published, each carrying the value that row holds
TEST_F(WithWriteTest, createsANodePerRowFromAPublishedValue) {
    applyWrite("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 "
               "CREATE (:Tag {name: name})");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"Adam"}, {"Cyrus"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
