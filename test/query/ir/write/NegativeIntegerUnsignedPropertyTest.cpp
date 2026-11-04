#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A count is an unsigned integer, so a cell holding one beside a cell holding an integer
// types a property UInt64, and an integer written to a UInt64 property is converted to
// it. A negative integer has no unsigned value: the write raises rather than wrap it.
// Neo4j has no unsigned type and stores 8 and -1 as integers.
class NegativeIntegerUnsignedPropertyTest : public WriteQueryTest {
protected:
    void expectWriteError(std::string_view query, std::string_view expectedError) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(NegativeIntegerUnsignedPropertyTest, widensAPositiveIntegerBesideACount) {
    applyWrite("MATCH (n:Person) WITH count(n) AS c UNWIND [c, 3] AS x CREATE (:T {v: x})");
    expectRows("MATCH (t:T) RETURN t.v", {{"8"}, {"3"}});
}

TEST_F(NegativeIntegerUnsignedPropertyTest, raisesOnANegativeIntegerBesideACount) {
    expectWriteError("MATCH (n:Person) WITH count(n) AS c UNWIND [c, -1] AS x CREATE (:T {v: x})",
                     "Cannot set a property of type 'UInt64' to the negative integer -1");
}

TEST_F(NegativeIntegerUnsignedPropertyTest, raisesOnANegativeIntegerSetToAnUnsignedProperty) {
    applyWrite("MATCH (n:Person) WITH count(n) AS c CREATE (:T {u: c})");

    applyWrite("MATCH (t:T) SET t.u = [3, 'a'][0]");
    expectRows("MATCH (t:T) RETURN t.u", {{"3"}});

    expectWriteError("MATCH (t:T) SET t.u = [-1, 'a'][0]",
                     "Cannot set a property of type 'UInt64' to the negative integer -1");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
