#include <gtest/gtest.h>

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"

#include "CallV3Test.h"
#include "ParameterTestHelpers.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

// The SimpleGraph fixture holds 15 INTERESTED_IN edges and 3 KNOWS_WELL edges
class EdgeTypeParameterTest : public CallV3Test {
protected:
    void expectCount(std::string_view query, const ParameterMap& parameters, std::string_view count) {
        StringRowSink sink;
        runQuery(query, parameters, sink);

        const std::vector<StringRowSink::Row> expected {{std::string(count)}};
        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(EdgeTypeParameterTest, aStringParameterNamesTheType) {
    ParameterMap parameters;
    setString(parameters, "type", "KNOWS_WELL");

    expectCount("MATCH ()-[e:$type]->() RETURN count(e)", parameters, "3");
}

// The disjunctions below take the 3 KNOWS_WELL edges and one MENTORS edge written first,
// a strict subset of the 19 edges: a dropped filter would count every edge
TEST_F(EdgeTypeParameterTest, aListParameterNamesADisjunction) {
    runWrite("MATCH (a {name: 'Luc'}), (b {name: 'Suhas'}) CREATE (a)-[:MENTORS]->(b)");

    ParameterMap parameters;
    const std::vector<std::string_view> types {"KNOWS_WELL", "MENTORS"};
    setStrings(parameters, "types", types);

    expectCount("MATCH ()-[e:$types]->() RETURN count(e)", parameters, "4");
}

TEST_F(EdgeTypeParameterTest, twoParametersJoinedByABar) {
    runWrite("MATCH (a {name: 'Luc'}), (b {name: 'Suhas'}) CREATE (a)-[:MENTORS]->(b)");

    ParameterMap parameters;
    setString(parameters, "first", "KNOWS_WELL");
    setString(parameters, "second", "MENTORS");

    expectCount("MATCH ()-[e:$first|$second]->() RETURN count(e)", parameters, "4");
}

TEST_F(EdgeTypeParameterTest, aParameterBesideALiteralType) {
    runWrite("MATCH (a {name: 'Luc'}), (b {name: 'Suhas'}) CREATE (a)-[:MENTORS]->(b)");

    ParameterMap parameters;
    setString(parameters, "type", "MENTORS");

    expectCount("MATCH ()-[e:$type|KNOWS_WELL]->() RETURN count(e)", parameters, "4");
}

TEST_F(EdgeTypeParameterTest, aTypeTestReadsTheParameter) {
    ParameterMap parameters;
    setString(parameters, "type", "KNOWS_WELL");

    expectCount("MATCH ()-[e]->() WHERE e:$type RETURN count(e)", parameters, "3");
}

TEST_F(EdgeTypeParameterTest, aParameterTypeOnAHopFromALabelledNode) {
    ParameterMap parameters;
    setString(parameters, "type", "KNOWS_WELL");

    StringRowSink sink;
    runQuery("MATCH (a:Person)-[:$type]->(b) RETURN b.name ORDER BY b.name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Adam"}, {"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The 3 KNOWS_WELL edges, and each one extended by a second KNOWS_WELL edge
TEST_F(EdgeTypeParameterTest, aParameterTypeOnAVariableLengthHop) {
    ParameterMap parameters;
    setString(parameters, "type", "KNOWS_WELL");

    expectCount("MATCH ()-[:$type*1..2]->() RETURN count(*)", parameters, "6");
}

TEST_F(EdgeTypeParameterTest, anUnknownTypeMatchesNothing) {
    ParameterMap parameters;
    setString(parameters, "type", "DISLIKES");

    expectCount("MATCH ()-[e:$type]->() RETURN count(e)", parameters, "0");
}

TEST_F(EdgeTypeParameterTest, createWritesTheParameterType) {
    ParameterMap parameters;
    setString(parameters, "type", "MENTORS");
    runWrite("MATCH (a {name: 'Luc'}), (b {name: 'Suhas'}) CREATE (a)-[:$type]->(b)", parameters);

    StringRowSink sink;
    runQuery("MATCH (a)-[e:MENTORS]->(b) RETURN a.name, b.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Luc", "Suhas"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(EdgeTypeParameterTest, aTypeTestReadsAnEdgeWrittenWithAParameter) {
    ParameterMap parameters;
    setString(parameters, "type", "ADVISES");

    StringRowSink sink;
    runWrite("CREATE (a:Mentor {name: 'Ana'})-[e:$type]->(b:Mentee {name: 'Bo'}) WITH e WHERE e:ADVISES RETURN count(e)", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(EdgeTypeParameterTest, aParameterTypeTestReadsAWrittenEdge) {
    ParameterMap parameters;
    setString(parameters, "type", "ADVISES");

    StringRowSink sink;
    runWrite("CREATE (a:Mentor {name: 'Ana'})-[e:ADVISES]->(b:Mentee {name: 'Bo'}) WITH e WHERE e:$type RETURN count(e)", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"1"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(EdgeTypeParameterTest, aTypeTestMissesAnEdgeWrittenWithAnotherParameter) {
    ParameterMap parameters;
    setString(parameters, "type", "ADVISES");

    StringRowSink sink;
    runWrite("CREATE (a:Mentor)-[e:$type]->(b:Mentee) WITH e WHERE e:KNOWS_WELL RETURN count(e)", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(EdgeTypeParameterTest, aParameterTypeTestMissesAWrittenEdge) {
    ParameterMap parameters;
    setString(parameters, "type", "KNOWS_WELL");

    StringRowSink sink;
    runWrite("CREATE (a:Mentor)-[e:ADVISES]->(b:Mentee) WITH e WHERE e:$type RETURN count(e)", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(EdgeTypeParameterTest, typeOfAnEdgeWrittenWithAParameter) {
    ParameterMap parameters;
    setString(parameters, "type", "ADVISES");

    StringRowSink sink;
    runWrite("CREATE (a:Mentor)-[e:$type]->(b:Mentee) RETURN type(e)", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"ADVISES"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(EdgeTypeParameterTest, aListIsRejectedInACreate) {
    ParameterMap parameters;
    const std::vector<std::string_view> types {"MENTORS", "ADVISES"};
    setStrings(parameters, "type", types);

    runWriteExpectingError("CREATE (a:Mentor)-[:$type]->(b:Mentee)",
                           parameters,
                           "Parameter $type has type List: an edge is written with one edge type, a String");
}

TEST_F(EdgeTypeParameterTest, twoTypesOnACreatedEdgeAreRejected) {
    ParameterMap parameters;
    setString(parameters, "type", "MENTORS");

    runWriteExpectingError("CREATE (a:Mentor)-[:$type|ADVISES]->(b:Mentee)", parameters, "An edge cannot have more than one edge type");
}

TEST_F(EdgeTypeParameterTest, anUndefinedParameterIsRejected) {
    const ParameterMap parameters;
    runQueryExpectingError("MATCH ()-[e:$type]->() RETURN e", parameters, "Parameter $type is not defined");
}

TEST_F(EdgeTypeParameterTest, anIntegerIsRejected) {
    ParameterMap parameters;
    ParameterValue number;
    number.setInt64(1);
    parameters.set("type", number);

    runQueryExpectingError("MATCH ()-[e:$type]->() RETURN e",
                           parameters,
                           "Parameter $type has type Int64: an edge type is a String or a list of Strings");
}
