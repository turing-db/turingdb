#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTestEnv.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Which CALL subqueries may follow an updating clause. The rule holds off a clause that
// goes to the graph for rows, since the write above it is still buffered and invisible; a
// body that only writes goes to the graph for none, so nothing is hidden from it
class SubqueryAfterUpdateTest : public WriteQueryTest {
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

TEST_F(SubqueryAfterUpdateTest, keepsAWriteOnlyBodyAfterACreate) {
    expectWriteRowCount("CREATE (n:A) CALL { CREATE (m:B) RETURN m } RETURN n, m", 1);

    expectRows("MATCH (a:A) RETURN count(a)", {{"1"}});
    expectRows("MATCH (b:B) RETURN count(b)", {{"1"}});
}

TEST_F(SubqueryAfterUpdateTest, keepsAWriteOnlyBodyAfterAnother) {
    expectWriteRowCount("CALL { CREATE (a:A) RETURN a } CALL { CREATE (b:B) RETURN b } RETURN a, b", 1);

    expectRows("MATCH (a:A) RETURN count(a)", {{"1"}});
    expectRows("MATCH (b:B) RETURN count(b)", {{"1"}});
}

// The other side of the line: the body matches, so the buffered write above it would be
// invisible to it
TEST_F(SubqueryAfterUpdateTest, rejectsABodyThatMatchesAfterACreate) {
    expectRejected("MATCH (p:Person) CREATE (:Audit) "
                   "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } RETURN count(k)",
                   "A reading clause cannot follow an updating clause");
}

// A MERGE matches before it creates, so a body ending on one goes to the graph for rows
TEST_F(SubqueryAfterUpdateTest, rejectsAMergingBodyAfterACreate) {
    expectRejected("CREATE (n:A) CALL { MERGE (m:B) RETURN m } RETURN n, m",
                   "A reading clause cannot follow an updating clause");
}

// A nested body answers for the one around it
TEST_F(SubqueryAfterUpdateTest, rejectsANestedBodyThatMatchesAfterACreate) {
    expectRejected("MATCH (p:Person) CREATE (:Audit) "
                   "CALL (p) { CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } RETURN k } RETURN count(k)",
                   "A reading clause cannot follow an updating clause");
}
