#include <gtest/gtest.h>

#include <stddef.h>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "ID.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// A sink for the statements that report no row.
class NullSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {}
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

}

// What a VECTOR SEARCH retrieves is a node the rest of the query may write to: the search
// picks the rows and a SET or a CREATE below it runs once per neighbour, inside a change.
class VectorSearchWriteTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _remy = SimpleGraph::findNodeID(graph, "Remy");
        _adam = SimpleGraph::findNodeID(graph, "Adam");

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink, ChangeID change = ChangeID::head()) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              change,
                              &_env->getMem(),
                              &sink);
    }

    void runQuery(std::string_view query, QueryStatus& status) {
        NullSink sink;
        runQuery(query, status, sink);
    }

    ChangeID openChange() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto opened = system.newChange(_graphName);
        bioassert(opened, "Failed to open a change");

        return opened.value()->id();
    }

    void submitChange(ChangeID change) {
        QueryCallbacks callbacks;
        const QueryState submitState(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), change);

        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << status.getError();
    }

    void runWrite(std::string_view query) {
        const ChangeID change = openChange();

        QueryStatus status;
        NullSink sink;
        runQuery(query, status, sink, change);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        submitChange(change);
    }

    void loadNodeVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "nodes.csv";

        std::ofstream file(path.get());
        file << _remy.getValue() << ",1,0,0,0\n" << _adam.getValue() << ",2,0,0,0\n";
        file.close();

        QueryStatus createStatus;
        runQuery("CREATE VECTOR INDEX nodes WITH DIMENSION 4 METRIC EUCLID", createStatus);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        runQuery("LOAD VECTOR FROM \"nodes.csv\" IN nodes", loadStatus);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << status.getError();

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::ranges::sort(sortedExpected);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected);
    }

    const std::string _graphName = "simpledb";
    NodeID _remy;
    NodeID _adam;
    QueryConfig _queryConfig;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VectorSearchWriteTest, setsAPropertyOfEveryNeighbourTheSearchReported) {
    loadNodeVectors();

    runWrite("VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids SET ids.age = 99");

    expectRows("MATCH (n:Person) WHERE n.age = 99 RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(VectorSearchWriteTest, createsAnEdgeOutOfTheNeighbourTheSearchReported) {
    loadNodeVectors();

    runWrite("VECTOR SEARCH IN nodes FOR 1 (1.0, 0.0, 0.0, 0.0) YIELD ids "
             "CREATE (ids)-[:RETRIEVED]->(t:Tag {name: 'Retrieved'})");

    expectRows("MATCH (n)-[:RETRIEVED]->(t) RETURN n.name, t.name", {{"Remy", "Retrieved"}});
}

// A retrieved node is deleted like any other, so one still carrying edges is refused until
// the query says DETACH DELETE.
TEST_F(VectorSearchWriteTest, deletingANeighbourThatStillCarriesEdgesIsRefused) {
    loadNodeVectors();

    const ChangeID change = openChange();

    QueryStatus status;
    NullSink sink;
    runQuery("VECTOR SEARCH IN nodes FOR 1 (1.0, 0.0, 0.0, 0.0) YIELD ids DELETE ids", status, sink, change);

    EXPECT_FALSE(status.isOk());
    EXPECT_NE(status.getError().find("DETACH DELETE"), std::string::npos) << status.getError();
}

// The index lives beside the graph rather than inside a commit, so a search reports the
// same neighbours whether the query runs at head or inside an open change.
TEST_F(VectorSearchWriteTest, searchesTheIndexFromInsideAnOpenChange) {
    loadNodeVectors();

    const ChangeID change = openChange();

    QueryStatus status;
    StringRowSink sink;
    runQuery("VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids.name", status, sink, change);

    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<StringRowSink::Row> expected {{"Remy"}, {"Adam"}};
    EXPECT_EQ(sink.getRows(), expected);
}
