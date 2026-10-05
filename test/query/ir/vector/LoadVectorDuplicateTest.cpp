#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "ID.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class LoadVectorDuplicateTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            Graph* graph = system.createGraph(_graphName);
            SimpleGraph::createSimpleGraph(graph);

            _remy = SimpleGraph::findNodeID(graph, "Remy");
            _adam = SimpleGraph::findNodeID(graph, "Adam");
        }

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        for (const std::string_view kind : INDEX_KINDS) {
            QueryStatus createStatus;
            runQuery("CREATE VECTOR INDEX " + indexName(kind) + " WITH DIMENSION 2 METRIC EUCLID TYPE " + std::string(kind),
                     createStatus);
            ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();
        }
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
    }

    void runQuery(std::string_view query, QueryStatus& status) {
        StringRowSink sink;
        runQuery(query, status, sink);
    }

    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / std::string(name);

        std::ofstream file(path.get());
        file << content;
    }

    void loadVectors(std::string_view index, std::string_view fileName, QueryStatus& status) {
        runQuery("LOAD VECTOR FROM \"" + std::string(fileName) + "\" IN " + indexName(index), status);
    }

    void expectNeighbours(std::string_view index, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery("VECTOR SEARCH IN " + indexName(index) + " FOR 2 (1.0, 0.0) YIELD ids, score RETURN ids.name, score",
                 status,
                 sink);

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(sink.getRows(), expected);
    }

    std::string indexName(std::string_view kind) {
        return "index" + std::string(kind);
    }

    std::string row(NodeID id, std::string_view values) {
        return std::to_string(id.getValue()) + "," + std::string(values) + "\n";
    }

    static constexpr std::string_view INDEX_KINDS[] = {"FLAT", "HNSW"};

    const std::string _graphName = "simpledb";
    NodeID _remy;
    NodeID _adam;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoadVectorDuplicateTest, loadingTheSameFileTwiceKeepsOneVectorPerID) {
    writeFile("two_rows.csv", row(_remy, "1,0") + row(_adam, "0,1"));

    for (const std::string_view index : INDEX_KINDS) {
        SCOPED_TRACE(index);

        QueryStatus firstStatus;
        loadVectors(index, "two_rows.csv", firstStatus);
        ASSERT_TRUE(firstStatus.isOk()) << firstStatus.getError();

        QueryStatus secondStatus;
        loadVectors(index, "two_rows.csv", secondStatus);
        ASSERT_TRUE(secondStatus.isOk()) << secondStatus.getError();

        expectNeighbours(index, {{"Remy", "0"}, {"Adam", "2"}});
    }
}

TEST_F(LoadVectorDuplicateTest, reloadingAnIDReplacesItsVector) {
    writeFile("two_rows.csv", row(_remy, "1,0") + row(_adam, "0,1"));
    writeFile("remy_flipped.csv", row(_remy, "-1,0"));

    for (const std::string_view index : INDEX_KINDS) {
        SCOPED_TRACE(index);

        QueryStatus firstStatus;
        loadVectors(index, "two_rows.csv", firstStatus);
        ASSERT_TRUE(firstStatus.isOk()) << firstStatus.getError();

        QueryStatus secondStatus;
        loadVectors(index, "remy_flipped.csv", secondStatus);
        ASSERT_TRUE(secondStatus.isOk()) << secondStatus.getError();

        expectNeighbours(index, {{"Adam", "2"}, {"Remy", "4"}});
    }
}

TEST_F(LoadVectorDuplicateTest, aFileOverlappingTheIndexAddsItsNewIDs) {
    writeFile("remy.csv", row(_remy, "1,0"));
    writeFile("adam_and_remy.csv", row(_adam, "0,1") + row(_remy, "-1,0"));

    for (const std::string_view index : INDEX_KINDS) {
        SCOPED_TRACE(index);

        QueryStatus firstStatus;
        loadVectors(index, "remy.csv", firstStatus);
        ASSERT_TRUE(firstStatus.isOk()) << firstStatus.getError();

        QueryStatus secondStatus;
        loadVectors(index, "adam_and_remy.csv", secondStatus);
        ASSERT_TRUE(secondStatus.isOk()) << secondStatus.getError();

        expectNeighbours(index, {{"Adam", "2"}, {"Remy", "4"}});
    }
}

TEST_F(LoadVectorDuplicateTest, aFileNamingAnIDTwiceIsRejected) {
    writeFile("repeated.csv", row(_remy, "1,0") + row(_adam, "0,1") + row(_remy, "0,1"));

    for (const std::string_view index : INDEX_KINDS) {
        SCOPED_TRACE(index);

        QueryStatus status;
        loadVectors(index, "repeated.csv", status);
        ASSERT_FALSE(status.isOk());
        EXPECT_NE(status.getError().find("Vector ID " + std::to_string(_remy.getValue()) + " appears more than once"),
                  std::string::npos)
            << status.getError();

        expectNeighbours(index, {});
    }
}

TEST_F(LoadVectorDuplicateTest, aFileOfNewIDsStillLoads) {
    writeFile("remy.csv", row(_remy, "1,0"));
    writeFile("adam.csv", row(_adam, "0,1"));

    for (const std::string_view index : INDEX_KINDS) {
        SCOPED_TRACE(index);

        QueryStatus firstStatus;
        loadVectors(index, "remy.csv", firstStatus);
        ASSERT_TRUE(firstStatus.isOk()) << firstStatus.getError();

        QueryStatus secondStatus;
        loadVectors(index, "adam.csv", secondStatus);
        ASSERT_TRUE(secondStatus.isOk()) << secondStatus.getError();

        expectNeighbours(index, {{"Remy", "0"}, {"Adam", "2"}});
    }
}
