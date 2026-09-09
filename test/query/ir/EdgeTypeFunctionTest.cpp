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

namespace {

// Remy's four outgoing edges, each beside its type
const std::vector<StringRowSink::Row> remyEdgeTypes = {
    {"Adam", "KNOWS_WELL"},
    {"Ghosts", "INTERESTED_IN"},
    {"Computers", "INTERESTED_IN"},
    {"Eighties", "INTERESTED_IN"},
};

}

// type(relationship) reads the type of an edge the pattern bound. The engine answers it
// under the name edgeType, so what is missing is the name openCypher gives it.
class EdgeTypeFunctionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
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

TEST_F(EdgeTypeFunctionTest, readsTheTypeOfAMatchedEdge) {
    expectRows("MATCH (n)-[e]->(m) WHERE n.name = 'Remy' RETURN m.name, type(e)", remyEdgeTypes);
}

TEST_F(EdgeTypeFunctionTest, readsTheTypeUnderAnAlias) {
    expectRows("MATCH (n)-[e]->(m) WHERE n.name = 'Remy' RETURN m.name, type(e) AS edge",
               remyEdgeTypes);
}

TEST_F(EdgeTypeFunctionTest, groupsOnTheTypeOfAnEdge) {
    expectRows("MATCH ()-[e]->() RETURN type(e), count(e)",
               {{"KNOWS_WELL", "3"}, {"INTERESTED_IN", "15"}});
}

TEST_F(EdgeTypeFunctionTest, filtersOnTheTypeOfAnEdge) {
    expectRows("MATCH (n)-[e]->(m) WHERE type(e) = 'KNOWS_WELL' RETURN n.name, m.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Ghosts", "Remy"}});
}

TEST_F(EdgeTypeFunctionTest, readsTheTypeUnderItsTuringName) {
    expectRows("MATCH (n)-[e]->(m) WHERE n.name = 'Remy' RETURN m.name, edgeType(e)",
               remyEdgeTypes);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
