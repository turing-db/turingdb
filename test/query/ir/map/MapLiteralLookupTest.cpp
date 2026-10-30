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
