#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class FailedWriteRollbackTest : public WriteQueryTest {
protected:
    void writeFailing(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        EXPECT_FALSE(status.isOk()) << "query: " << query;

        submit(changeID);
    }
};

TEST_F(FailedWriteRollbackTest, keepsNoNodeAQueryCreatedBeforeFailing) {
    writeFailing("OPTIONAL MATCH (n:Nope) CREATE (n)-[:SEES]->(:Stray)");

    expectRows("MATCH (n:Stray) RETURN count(n)", {{"0"}});
}

TEST_F(FailedWriteRollbackTest, keepsNoValueAQuerySetBeforeFailing) {
    writeFailing("MATCH (p:Person {name: 'Remy'}) SET p.age = 1 "
                 "WITH p OPTIONAL MATCH (n:Nope) CREATE (p)-[:SEES]->(n)");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"32"}});
}

// Doruk holds 1 edge in simpledb
TEST_F(FailedWriteRollbackTest, keepsNoDeletionAQueryMadeBeforeFailing) {
    writeFailing("MATCH (d:Person {name: 'Doruk'}) DETACH DELETE d "
                 "WITH count(*) AS deleted OPTIONAL MATCH (n:Nope) CREATE (n)-[:SEES]->(:Stray)");

    expectRows("MATCH (d:Person {name: 'Doruk'})-[e]-() RETURN count(e)", {{"1"}});
}

TEST_F(FailedWriteRollbackTest, keepsTheWritesOfTheQueriesBeforeTheFailedOne) {
    ChangeID changeID;
    openChange(changeID);

    const QueryStatus created = runWrite("CREATE (:Kept {name: 'k'})", changeID);
    EXPECT_TRUE(created.isOk()) << created.getError();

    const QueryStatus failed = runWrite("OPTIONAL MATCH (n:Nope) CREATE (n)-[:SEES]->(:Stray)", changeID);
    EXPECT_FALSE(failed.isOk());

    submit(changeID);

    expectRows("MATCH (n:Kept) RETURN n.name", {{"k"}});
    expectRows("MATCH (n:Stray) RETURN count(n)", {{"0"}});
}

// The failed query typed tag as an integer from its first row; taking the query back takes
// that type back too, so the next query of the change types it as a string
TEST_F(FailedWriteRollbackTest, keepsNoPropertyTypeAFailedQueryCreated) {
    ChangeID changeID;
    openChange(changeID);

    const QueryStatus failed = runWrite("UNWIND [1, 'x'] AS e CREATE (:Stray {tag: e})", changeID);
    EXPECT_FALSE(failed.isOk());

    const QueryStatus created = runWrite("CREATE (:Kept {tag: 'x'})", changeID);
    EXPECT_TRUE(created.isOk()) << created.getError();

    submit(changeID);

    expectRows("MATCH (n:Kept) RETURN n.tag", {{"x"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
