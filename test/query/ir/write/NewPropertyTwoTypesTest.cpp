#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// One write whose values share no type, reaching a property that has none yet, raises
// naming the two types it would give the property. A write clashing with a type the
// property does have keeps the message naming that type. Neo4j stores mixed kinds row by
// row, so it raises on none of these.
class NewPropertyTwoTypesTest : public WriteQueryTest {
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

TEST_F(NewPropertyTwoTypesTest, namesANumberAndAString) {
    expectWriteError("UNWIND [1, 'a'] AS x CREATE (:T {v: x})",
                     "One write gives the new property 'v' two types: 'Integer' and 'String'");
}

TEST_F(NewPropertyTwoTypesTest, namesAListAndAMap) {
    expectWriteError("UNWIND [[1], {a: 1}] AS x CREATE (:T {v: x})",
                     "One write gives the new property 'v' two types: 'List' and 'Map'");
}

TEST_F(NewPropertyTwoTypesTest, namesTheTypesOfACase) {
    expectWriteError("MATCH (n:Person) SET n.v = CASE WHEN n.isFrench THEN 1 ELSE 'a' END",
                     "One write gives the new property 'v' two types: 'Integer' and 'String'");
}

TEST_F(NewPropertyTwoTypesTest, keepsTheMessageOfTwoTypedWrites) {
    expectWriteError("CREATE (:T {v: 1}), (:U {v: 'a'})",
                     "Property type 'v' already exists with a different type 'Int64' vs. 'String'");
}

TEST_F(NewPropertyTwoTypesTest, keepsTheMessageOfAnExistingType) {
    applyWrite("CREATE (:T {v: 1})");
    expectWriteError("MATCH (t:T) UNWIND [1, 'a'] AS x SET t.v = x",
                     "Cannot set a property of type 'Int64' to a value of type 'String'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
