#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class MergeCallSubqueryTest : public WriteQueryTest {
protected:
    void expectWriteRejected(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(MergeCallSubqueryTest, returnsAnEntityTheBodyMerged) {
    expectWriteRows("CALL { MERGE (n:Person {name: 'Nia'}) RETURN n } RETURN n.name", {{"Nia"}});
}

TEST_F(MergeCallSubqueryTest, returnsAnEntityTheBodyMergedPerRow) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { MERGE (n:Person {name: name}) RETURN n } "
                    "RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeCallSubqueryTest, returnsAnImportedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n CALL (n) { RETURN n AS m } RETURN m.name, m.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeCallSubqueryTest, returnsAPropertyOfAnEntityTheBodyMerged) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { MERGE (n:Person {name: name}) RETURN n.age AS age } "
                    "RETURN name, age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeCallSubqueryTest, returnsAnEntityTheBodyCreated) {
    expectWriteRows("CALL { CREATE (n:Person {name: 'Kai'}) RETURN n } RETURN n.name", {{"Kai"}});
}

TEST_F(MergeCallSubqueryTest, returnsAnEntityMergedTwoSubqueriesDown) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { CALL (name) { MERGE (n:Person {name: name}) RETURN n } RETURN n AS m } "
                    "RETURN m.name, m.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeCallSubqueryTest, setsAPropertyOfAReturnedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { MERGE (n:Person {name: name}) RETURN n } "
                    "SET n.age = 50 RETURN n.name, n.age",
                    {{"Nia", "50"}, {"Remy", "50"}});
}

TEST_F(MergeCallSubqueryTest, padsAnOptionalCallReturningAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { MERGE (n:Person {name: name}) WITH n WHERE n.age > 30 RETURN n } "
                    "RETURN name, n.name, n.age",
                    {{"Nia", "null", "null"}, {"Remy", "Remy", "32"}});
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { MERGE (n:Person {name: name}) WITH n WHERE n.age IS NULL RETURN n } "
                    "RETURN name, n.name, n:Person",
                    {{"Nia", "Nia", "true"}, {"Remy", "null", "false"}});
}

TEST_F(MergeCallSubqueryTest, setsAPropertyOfAPaddedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { MERGE (n:Person {name: name}) WITH n WHERE n.age IS NULL RETURN n } "
                    "SET n.age = 7 RETURN name, n.age",
                    {{"Nia", "7"}, {"Remy", "null"}});
}

TEST_F(MergeCallSubqueryTest, rejectsGroupingByAMergedEntity) {
    expectWriteRejected("UNWIND ['Remy', 'Nia'] AS name "
                        "CALL (name) { MERGE (n:Person {name: name}) RETURN n, count(*) AS c } "
                        "RETURN n.name, c",
                        "A RETURN cannot group by 'n'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
