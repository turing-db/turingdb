#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A clause reading after a CALL whose body writes reads what every run of the body wrote
class ReadAfterWritingCallTest : public WriteQueryTest {
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

TEST_F(ReadAfterWritingCallTest, matchesWhatEveryRunOfAUnitBodyWrote) {
    applyWrite("CREATE (:Counter {c: 0})");

    expectWriteRows("UNWIND [1, 2] AS x CALL () { MATCH (n:Counter) SET n.c = n.c + 1 } MATCH (m:Counter) RETURN x, m.c",
                    {{"1", "2"}, {"2", "2"}});
}

TEST_F(ReadAfterWritingCallTest, readsInAReturningCallWhatAnEarlierBodyWrote) {
    applyWrite("CREATE (:Counter {c: 0})");

    expectWriteRows("UNWIND [1, 2] AS x "
                    "CALL () { MATCH (n:Counter) SET n.c = n.c + 1 } "
                    "CALL () { MATCH (n:Counter) RETURN n.c AS c } "
                    "RETURN x, c",
                    {{"1", "2"}, {"2", "2"}});
}

TEST_F(ReadAfterWritingCallTest, needsAReturnAfterTheRead) {
    expectRejected("CALL () { CREATE (:Audit) } MATCH (m:Audit)", "Return statement is missing");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
