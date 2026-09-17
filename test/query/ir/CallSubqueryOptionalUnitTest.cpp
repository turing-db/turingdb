#include <gtest/gtest.h>

#include <string_view>

#include "QueryStatus.h"
#include "versioning/ChangeID.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// OPTIONAL pads the input rows a body yields nothing for. A unit body yields nothing for
// every row and leaves the rows as they are, so there is no row for OPTIONAL to pad and
// nothing the keyword can mean: the query is rejected rather than run as if it were absent
class CallSubqueryOptionalUnitTest : public WriteQueryTest {
protected:
    void expectWriteRejected(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(CallSubqueryOptionalUnitTest, optionalOnAUnitBodyIsRejected) {
    expectWriteRejected("MATCH (p:Person) OPTIONAL CALL (p) { CREATE (:Audit) }", "OPTIONAL");
}
