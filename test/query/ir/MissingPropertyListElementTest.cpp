#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class MissingPropertyListElementTest : public CallV3Test {
};

TEST_F(MissingPropertyListElementTest, comparesAnElementBesideAMissingPropertyToAnInteger) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) RETURN [0, n.missing][0] - 0 = 3", sink);

    const std::vector<StringRowSink::Row> expected {{"false"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(MissingPropertyListElementTest, comparesAnElementBesideAMissingPropertyToAProperty) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) RETURN [0, n.missing][0] + 0 = n.age", sink);

    const std::vector<StringRowSink::Row> expected {{"false"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(MissingPropertyListElementTest, comparesAnElementBesideAMissingPropertyToAListElement) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) RETURN [0, n.missing][0] - 0 = [2][0]", sink);

    const std::vector<StringRowSink::Row> expected {{"false"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(MissingPropertyListElementTest, indexesAListByAnElementBesideAMissingProperty) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) RETURN [0][[0, n.missing][0] + 0]", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(MissingPropertyListElementTest, testsAMissingPropertyElementForMembershipInAnother) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) RETURN [n.missing][0] IN [n.missing][0]", sink);

    const std::vector<StringRowSink::Row> expected {{"null"}};
    EXPECT_EQ(sink.getRows(), expected);
}
