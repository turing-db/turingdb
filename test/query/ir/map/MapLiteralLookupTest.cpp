#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A property lookup reads a key straight off a map literal, as it reads one off a map held
// in a variable: the value under the key, or null when the map has no such key.
class MapLiteralLookupTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MapLiteralLookupTest, readsAKeyOfAMapLiteral) {
    expectRows("RETURN {k: 1}.k", {{"1"}});
    expectRows("RETURN {a: 1, b: 2}.b", {{"2"}});
}

TEST_F(MapLiteralLookupTest, readsAKeyAMapLiteralLacksAsNull) {
    expectRows("RETURN {k: 1}.missing", {{"null"}});
}

TEST_F(MapLiteralLookupTest, readsAKeyOfAMapLiteralBuiltFromTheRow) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN {k: p.name}.k", {{"Remy"}});
}

TEST_F(MapLiteralLookupTest, readsAPropertyOfTheNodeAMapLiteralHolds) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN {k: p}.k.name", {{"Remy"}});
}

TEST_F(MapLiteralLookupTest, readsAKeyOfAParenthesizedMapLiteral) {
    expectRows("RETURN ({k: 1}).k", {{"1"}});
}

TEST_F(MapLiteralLookupTest, readsAKeyOfTheMapAMapLiteralHolds) {
    expectRows("RETURN {k: {j: 2}}.k.j", {{"2"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN {k: {j: p}}.k.j.name", {{"Remy"}});
}

TEST_F(MapLiteralLookupTest, readsAPropertyOfTheEdgeAMapLiteralHolds) {
    expectRows("MATCH (p:Person {name: 'Remy'})-[e]->(q:Person {name: 'Adam'}) RETURN {e: e}.e.name",
               {{"Remy -> Adam"}});
}

TEST_F(MapLiteralLookupTest, readsAKeyOfTheMapACaseGives) {
    expectRows("RETURN CASE WHEN true THEN {k: 1} ELSE {k: 2} END.k", {{"1"}});
}

TEST_F(MapLiteralLookupTest, readsAKeyOfTheMapAReduceGives) {
    expectRows("RETURN reduce(acc = {k: 0}, x IN [1, 2] | {k: acc.k + x}).k", {{"3"}});
}

TEST_F(MapLiteralLookupTest, readsAPropertyOfTheNodeACollectSubqueryGives) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN COLLECT { MATCH (q:Person {name: 'Remy'}) RETURN q }[0].name",
               {{"Remy"}});
}

TEST_F(MapLiteralLookupTest, readsAPropertyOfAParenthesizedValue) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH {k: p} AS m RETURN (m.k).name", {{"Remy"}});
    expectRows("RETURN (datetime('2024-01-02T00:00:00Z')).year", {{"2024"}});
}

TEST_F(MapLiteralLookupTest, rejectsAPropertyOfAValueThatHasNone) {
    expectError("RETURN (1 + 2).foo", "A value of type 'Integer' has no property 'foo'");
    expectError("RETURN [1, 2].foo", "A value of type 'List' has no property 'foo'");
}
