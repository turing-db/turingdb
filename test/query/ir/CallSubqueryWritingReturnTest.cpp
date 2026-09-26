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

// A body that writes has written whatever it ends on, and the clause after it reads what
// every run of it wrote. A bare CREATE still needs a WITH before a read.
class CallSubqueryWritingReturnTest : public WriteQueryTest {
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

TEST_F(CallSubqueryWritingReturnTest, readsAfterABodyThatWroteAndReturned) {
    expectWriteRows("MATCH (p:Person) CALL (p) { CREATE (a:Audit) RETURN a } "
                    "MATCH (m:Audit) RETURN count(m)",
                    {{"64"}});
}

// The same query without the subquery, which the rule rejects
TEST_F(CallSubqueryWritingReturnTest, rejectsAReadAfterABareCreate) {
    expectRejected("MATCH (p:Person) CREATE (a:Audit) MATCH (m:Audit) RETURN count(m)",
                   "A reading clause cannot follow an updating clause");
}

TEST_F(CallSubqueryWritingReturnTest, aWithSeparatesTheWriteFromTheRead) {
    expectWriteRows("MATCH (p:Person) CALL (p) { CREATE (a:Audit) RETURN a } "
                    "WITH p MATCH (m:Audit) RETURN count(m)",
                    {{"64"}});
}

TEST_F(CallSubqueryWritingReturnTest, keepsAReadAfterABodyThatOnlyReads) {
    expectRows("MATCH (p:Person) CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } "
               "MATCH (m:Person) RETURN count(m)",
               {{"16"}});
}

// A returning body is a reading clause, so an updating clause before it is rejected too
TEST_F(CallSubqueryWritingReturnTest, rejectsAReturningBodyAfterAWrite) {
    expectRejected("MATCH (p:Person) CREATE (:Audit) "
                   "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } RETURN count(k)",
                   "A reading clause cannot follow an updating clause");
}
