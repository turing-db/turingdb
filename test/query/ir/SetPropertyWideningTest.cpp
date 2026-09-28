#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A write whose value is not of the type the schema holds the property as. The analyzer
// lets an integer reach a Double or UInt64 property (ExprAnalyzer::propTypeCompatible),
// so the write converts it to the property's type before staging it.
class SetPropertyWideningTest : public CallV3Test {
};

TEST_F(SetPropertyWideningTest, widensAnIntegerWrittenToADoubleNodeProperty) {
    runWrite("MATCH (p:Person {name: 'Remy'}) SET p.height = 1.5");
    runWrite("MATCH (p:Person {name: 'Remy'}) SET p.height = 2");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Remy'}) RETURN p.height + 0.25", read);

    const std::vector<StringRowSink::Row> expectedRead {{"2.25"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetPropertyWideningTest, widensAnIntegerWrittenToADoubleEdgeProperty) {
    runWrite("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) SET e.weight = 1.5");
    runWrite("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) SET e.weight = 2");

    StringRowSink read;
    runQuery("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) RETURN e.weight + 0.25", read);

    const std::vector<StringRowSink::Row> expectedRead {{"2.25"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetPropertyWideningTest, widensAnIntegerACreatePatternWritesToADoubleProperty) {
    runWrite("MATCH (p:Person {name: 'Remy'}) SET p.height = 1.5");
    runWrite("CREATE (p:Person {name: 'Tall', height: 2})");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Tall'}) RETURN p.height + 0.25", read);

    const std::vector<StringRowSink::Row> expectedRead {{"2.25"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetPropertyWideningTest, widensAnIntegerAMergePatternWritesToADoubleProperty) {
    runWrite("MATCH (p:Person {name: 'Remy'}) SET p.height = 1.5");
    runWrite("MERGE (p:Person {name: 'Tall', height: 2})");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Tall'}) RETURN p.height + 0.25", read);

    const std::vector<StringRowSink::Row> expectedRead {{"2.25"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
