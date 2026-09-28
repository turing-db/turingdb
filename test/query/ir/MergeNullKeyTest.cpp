#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class MergeNullKeyTest : public WriteQueryTest {
protected:
    void expectMergeRejected(std::string_view query, std::string_view propertyName) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        EXPECT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(propertyName), std::string::npos) << status.getError();

        submit(changeID);
    }
};

// Luc carries no age in simpledb
TEST_F(MergeNullKeyTest, rejectsANodeKeyThatIsNullOnARow) {
    expectMergeRejected("MATCH (p:Person {name: 'Luc'}) MERGE (n:Tally {age: p.age}) RETURN n", "age");

    expectRows("MATCH (n:Tally) RETURN count(n)", {{"0"}});
}

TEST_F(MergeNullKeyTest, rejectsAnUnwoundNodeKeyThatIsNull) {
    expectMergeRejected("UNWIND ['k1', null] AS k MERGE (n:Tally {name: k})", "name");
}

TEST_F(MergeNullKeyTest, rejectsAnEdgeKeyThatIsNullOnARow) {
    expectMergeRejected("MATCH (a:Person {name: 'Luc'}), (b:Person {name: 'Remy'}) "
                        "MERGE (a)-[:SEES {years: a.age}]->(b)",
                        "years");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
