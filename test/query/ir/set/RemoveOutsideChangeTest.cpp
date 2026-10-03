#include <gtest/gtest.h>

#include <string>

#include "QueryStatus.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class RemoveOutsideChangeTest : public WriteQueryTest {
};

TEST_F(RemoveOutsideChangeTest, namesRemoveInTheErrorForARemoveOutsideAChange) {
    RowSink sink;
    const QueryStatus status = runQuery("MATCH (p:Person {name: 'Remy'}) REMOVE p.age", &sink);
    ASSERT_FALSE(status.isOk());

    const std::string& error = status.getError();

    EXPECT_NE(error.find("Cannot perform SET or REMOVE outside of a write transaction."), std::string::npos)
        << "error: " << error;
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
