#include "PointImportTest.h"

#include <gtest/gtest.h>

#include <fstream>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

PointImportTest::PointImportTest() {
}

PointImportTest::~PointImportTest() {
}

void PointImportTest::initialize() {
    _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* graph = system.createGraph(_sessionGraph);
    SimpleGraph::createSimpleGraph(graph);

    _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
}

void PointImportTest::getDataDir(FileUtils::Path& dataDir) const {
    dataDir = FileUtils::Path {_env->getConfig().getDataDir().get()};
}

void PointImportTest::writeFixture(std::string_view fileName, std::string_view contents) {
    FileUtils::Path dataDir;
    getDataDir(dataDir);

    std::ofstream file(dataDir / std::string {fileName});
    ASSERT_TRUE(file.is_open());

    file << contents;
}

void PointImportTest::runQuery(std::string_view query, std::string_view graphName, NLOutputSink& sink) {
    QueryStatus status;
    _interpreter->execute(status,
                          query,
                          graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);

    ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
}

void PointImportTest::load(std::string_view query) {
    StringRowSink sink;
    runQuery(query, _sessionGraph, sink);
}

void PointImportTest::runQueryExpectingError(std::string_view query, std::string_view reason) {
    StringRowSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          query,
                          _sessionGraph,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);

    ASSERT_FALSE(status.isOk()) << "accepted: " << query;

    const std::string error = status.getError();
    EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
}

void PointImportTest::expectSortedRows(std::string_view query,
                                       std::string_view graphName,
                                       const std::vector<StringRowSink::Row>& expected) {
    StringRowSink sink;
    runQuery(query, graphName, sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected) << query;
}
