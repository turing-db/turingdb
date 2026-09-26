#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class CreateInvalidPatternTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(CreateInvalidPatternTest, rejectsAnUndirectedEdge) {
    expectRejected("CREATE (a:X)-[:R]-(b:X)", "Only directed relationships are supported in CREATE");
}

TEST_F(CreateInvalidPatternTest, rejectsCreatingANodeTheMatchBound) {
    expectRejected("MATCH (a:Person {name: 'Remy'}) CREATE (a)", "Variable 'a' already declared");
}

TEST_F(CreateInvalidPatternTest, rejectsCreatingANodeAnEarlierPatternCreated) {
    expectRejected("CREATE (a:X), (a)", "Variable 'a' already declared");
}

TEST_F(CreateInvalidPatternTest, rejectsMergingANodeTheMatchBound) {
    expectRejected("MATCH (a:Person {name: 'Remy'}) MERGE (a)", "Variable 'a' already declared");
}

TEST_F(CreateInvalidPatternTest, createsAnEdgeFromANodeTheMatchBound) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}) CREATE (a)-[:R]->(b:X {k: 1}) RETURN b.k", {{"1"}});
}

TEST_F(CreateInvalidPatternTest, mergesAnUndirectedEdge) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) MERGE (a)-[:T]-(b) RETURN b.name",
                    {{"Adam"}});

    expectRows("MATCH (:Person {name: 'Remy'})-[:T]->(b) RETURN b.name", {{"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
