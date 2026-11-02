#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// labels() returns a list of strings, as keys() and split() do, so an element read out of
// it is a String: it has no property to read, and the string functions take it as one.
// Remy is a Person, a SoftwareEngineering and a Founder, in that order.
class FunctionListElementTypeTest : public WriteQueryTest {
};

TEST_F(FunctionListElementTypeTest, rejectsAPropertyOfALabelBoundByAWith) {
    expectError("MATCH (n) WITH labels(n)[0] AS l RETURN l.x LIMIT 1",
                "Variable 'l' is 'String' it must be a node or edge");
}

TEST_F(FunctionListElementTypeTest, rejectsAPropertyOfAnIndexedLabel) {
    expectError("MATCH (n) RETURN (labels(n)[0]).x",
                "A value of type 'String' has no property 'x'");
}

TEST_F(FunctionListElementTypeTest, rejectsAPropertyOfTheHeadAndLastLabel) {
    expectError("MATCH (n) RETURN head(labels(n)).x",
                "A value of type 'String' has no property 'x'");
    expectError("MATCH (n) RETURN last(labels(n)).x",
                "A value of type 'String' has no property 'x'");
}

TEST_F(FunctionListElementTypeTest, rejectsAPropertyOfAnUnwoundLabel) {
    expectError("MATCH (n) UNWIND labels(n) AS l RETURN l.x",
                "Variable 'l' is 'String' it must be a node or edge");
}

TEST_F(FunctionListElementTypeTest, rejectsAPropertyOfALabelInAComprehension) {
    expectError("MATCH (n) RETURN [l IN labels(n) | l.x]",
                "Variable 'l' is 'String' it must be a node or edge");
}

TEST_F(FunctionListElementTypeTest, rejectsAPropertyOfALabelOfACell) {
    expectError("MATCH (n) WITH [n, 1] AS xs RETURN (labels(xs[0])[0]).x",
                "A value of type 'String' has no property 'x'");
}

TEST_F(FunctionListElementTypeTest, rejectsAPropertyOfAKeyAndOfASplitPart) {
    expectError("MATCH (n) RETURN (keys(n)[0]).x",
                "A value of type 'String' has no property 'x'");
    expectError("RETURN (split('a,b', ',')[0]).x",
                "A value of type 'String' has no property 'x'");
}

TEST_F(FunctionListElementTypeTest, readsALabelAsAString) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN labels(n)[0], toUpper(labels(n)[0]), size(labels(n)[0]), labels(n)[1] + '!'",
               {{"Person", "PERSON", "6", "SoftwareEngineering!"}});
}

TEST_F(FunctionListElementTypeTest, readsTheLabelsOfACellAsStrings) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [n, 1] AS xs RETURN toLower(labels(xs[0])[2])",
               {{"founder"}});
}

TEST_F(FunctionListElementTypeTest, unwindsTheLabelsAsStrings) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND labels(n) AS l RETURN l, l STARTS WITH 'S'",
               {{"Founder", "false"}, {"Person", "false"}, {"SoftwareEngineering", "true"}});
}

TEST_F(FunctionListElementTypeTest, mapsTheLabelsInAComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [l IN labels(n) | toLower(l)], head(tail(labels(n)))",
               {{"[person, softwareengineering, founder]", "SoftwareEngineering"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
