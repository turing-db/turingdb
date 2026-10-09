#include <gtest/gtest.h>

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

void setString(ParameterMap& parameters, std::string_view name, std::string_view value) {
    ParameterValue parameter;
    parameter.setString(value);
    parameters.set(name, parameter);
}

void setStrings(ParameterMap& parameters, std::string_view name, std::span<const std::string_view> values) {
    ParameterValue parameter;
    ParameterValue::List& list = parameter.setList();
    for (const std::string_view value : values) {
        list.emplace_back().setString(value);
    }

    parameters.set(name, parameter);
}

}

class LabelParameterTest : public CallV3Test {
};

TEST_F(LabelParameterTest, aStringParameterNamesTheLabel) {
    ParameterMap parameters;
    setString(parameters, "label", "Founder");

    StringRowSink sink;
    runQuery("MATCH (n:$label) RETURN n.name ORDER BY n.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(LabelParameterTest, aListParameterNamesEveryLabel) {
    ParameterMap parameters;
    const std::vector<std::string_view> labels {"Interest", "Exotic"};
    setStrings(parameters, "labels", labels);

    StringRowSink sink;
    runQuery("MATCH (n:$labels) RETURN n.name ORDER BY n.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Eighties"}, {"Ghosts"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(LabelParameterTest, aParameterBesideALiteralLabel) {
    ParameterMap parameters;
    setString(parameters, "label", "Person");

    StringRowSink sink;
    runQuery("MATCH (n:$label:SoftwareEngineering) RETURN n.name ORDER BY n.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Cyrus"}, {"Luc"}, {"Remy"}, {"Suhas"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(LabelParameterTest, aLabelPredicateReadsTheParameter) {
    ParameterMap parameters;
    setString(parameters, "label", "Founder");

    StringRowSink sink;
    runQuery("MATCH (n) WHERE n:$label RETURN n.name ORDER BY n.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(LabelParameterTest, aNumberedParameter) {
    ParameterMap parameters;
    setString(parameters, "0", "Founder");

    StringRowSink sink;
    runQuery("MATCH (n:$0) RETURN n.name ORDER BY n.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(LabelParameterTest, createWritesTheParameterLabels) {
    ParameterMap parameters;
    const std::vector<std::string_view> labels {"Planet", "Dwarf"};
    setStrings(parameters, "labels", labels);
    runWrite("CREATE (n:$labels {name: 'Pluto'})", parameters);

    StringRowSink sink;
    runQuery("MATCH (n:Planet:Dwarf) RETURN n.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Pluto"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(LabelParameterTest, anUnknownLabelMatchesNothing) {
    ParameterMap parameters;
    setString(parameters, "label", "Comet");

    StringRowSink sink;
    runQuery("MATCH (n:$label) RETURN n.name", parameters, sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(LabelParameterTest, anUndefinedParameterIsRejected) {
    const ParameterMap parameters;
    runQueryExpectingError("MATCH (n:$label) RETURN n", parameters, "Parameter $label is not defined");
}

TEST_F(LabelParameterTest, aQueryWithoutParametersRejectsTheReference) {
    runQueryExpectingError("MATCH (n:$label) RETURN n", "Parameter $label is not defined");
}

TEST_F(LabelParameterTest, anIntegerIsRejected) {
    ParameterMap parameters;
    ParameterValue age;
    age.setInt64(32);
    parameters.set("label", age);

    runQueryExpectingError("MATCH (n:$label) RETURN n",
                           parameters,
                           "Parameter $label has type Int64: a label is a String or a list of Strings");
}

TEST_F(LabelParameterTest, aNullIsRejected) {
    ParameterMap parameters;
    ParameterValue null;
    null.setNull();
    parameters.set("label", null);

    runQueryExpectingError("MATCH (n:$label) RETURN n",
                           parameters,
                           "Parameter $label is null: a label is a String or a list of Strings");
}

TEST_F(LabelParameterTest, aListWithANonStringIsRejected) {
    ParameterMap parameters;
    ParameterValue mixed;
    ParameterValue::List& list = mixed.setList();
    list.emplace_back().setString("Person");
    list.emplace_back().setInt64(1);
    parameters.set("labels", mixed);

    runQueryExpectingError("MATCH (n:$labels) RETURN n",
                           parameters,
                           "Element 1 of parameter $labels has type Int64: a label is a String");
}

TEST_F(LabelParameterTest, anEmptyListIsRejected) {
    ParameterMap parameters;
    ParameterValue none;
    none.setList();
    parameters.set("labels", none);

    runQueryExpectingError("MATCH (n:$labels) RETURN n", parameters, "Parameter $labels holds no label");
}

TEST_F(LabelParameterTest, anEmptyNameIsRejected) {
    ParameterMap parameters;
    setString(parameters, "label", "");

    runQueryExpectingError("MATCH (n:$label) RETURN n", parameters, "Parameter $label holds an empty label name");
}

TEST_F(LabelParameterTest, aLabelTestReadsANodeWrittenWithAParameter) {
    ParameterMap parameters;
    setString(parameters, "label", "Planet");

    StringRowSink sink;
    runWrite("CREATE (n:$label {name: 'Pluto'}) WITH n WHERE n:Planet RETURN n.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Pluto"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(LabelParameterTest, aLabelTestMissesANodeWrittenWithAnotherParameter) {
    ParameterMap parameters;
    setString(parameters, "label", "Planet");

    StringRowSink sink;
    runWrite("CREATE (n:$label {name: 'Ceres'}) WITH n WHERE n:Interest RETURN n.name", parameters, sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(LabelParameterTest, aParameterLabelTestReadsAWrittenNode) {
    ParameterMap parameters;
    setString(parameters, "label", "Comet");

    StringRowSink sink;
    runWrite("CREATE (n:Comet {name: 'Halley'}) WITH n WHERE n:$label RETURN n.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Halley"}};
    EXPECT_EQ(sink.getRows(), expected);
}
