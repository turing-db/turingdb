#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "FileUtils.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using ViewColumn = ColumnVector<types::String::Primitive>;

// Naming the graph it built is all LOAD PARQUET itself reports; what it imported is read
// back through that graph.
class GraphNameSink : public NLOutputSink {
public:
    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override {
        _columnNames.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const ViewColumn* const names = static_cast<const ViewColumn*>(chunks.front());

        for (size_t row = offset; row < offset + rowCount; row++) {
            _names.emplace_back((*names)[row]);
        }
    }

    const std::vector<std::string>& getColumnNames() const { return _columnNames; }
    const std::vector<std::string>& getNames() const { return _names; }

private:
    std::vector<std::string> _columnNames;
    std::vector<std::string> _names;
};

}

// LOAD PARQUET runs through the MLIR engine as a db.import_graph op carrying the parquet
// format. The fixture pair it reads is the one the importer's own tests use: four nodes
// across three labels, three edges, and a property of every supported type.
class LoadParquetV3Test : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_sessionGraph);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    // Lays the fixture pair out as <dataDir>/<directoryName>/{nodes,edges}.parquet, which
    // is the split export shape LOAD PARQUET resolves a bare directory name to.
    void writeFixtureDirectory(std::string_view directoryName) {
        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / std::string {directoryName};
        FileUtils::createDirectory(importDir);

        const FileUtils::Path fixtureDir {PARQUET_TEST_DATA_DIR};
        FileUtils::copy(fixtureDir / "nodes.parquet", importDir / "nodes.parquet");
        FileUtils::copy(fixtureDir / "edges.parquet", importDir / "edges.parquet");
    }

    void runQuery(std::string_view query, std::string_view graphName, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void runQueryExpectingError(std::string_view query, std::string_view reason) {
        GraphNameSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _sessionGraph,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    void expectSortedRows(std::string_view query,
                          std::string_view graphName,
                          const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, graphName, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << query;
    }

    // Every part of the export the importer has to carry over: the nodes and their
    // properties, the labels they were exported under, and the typed edges between them.
    void expectImported(std::string_view graphName) {
        expectSortedRows("MATCH (n) RETURN n.nodeString",
                         graphName,
                         {{"acme"}, {"alice"}, {"bob"}, {"carol"}});

        expectSortedRows("MATCH (n:Employee) RETURN n.nodeString, n.nodeInt",
                         graphName,
                         {{"bob", "20"}, {"carol", "40"}});

        expectSortedRows("MATCH ()-[r]->() RETURN r.edgeString",
                         graphName,
                         {{"e0"}, {"e1"}, {"e2"}});

        expectSortedRows("MATCH ()-[r:WORKS_FOR]->() RETURN r.edgeString, r.edgeInt",
                         graphName,
                         {{"e1", "200"}});
    }

    const std::string _sessionGraph = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoadParquetV3Test, importsTheSplitExportUnderTheNameAsGives) {
    writeFixtureDirectory("typed");

    GraphNameSink sink;
    runQuery("LOAD PARQUET 'typed' AS people", _sessionGraph, sink);

    ASSERT_EQ(sink.getNames().size(), 1u);
    EXPECT_EQ(sink.getColumnNames().front(), "graphName");
    EXPECT_EQ(sink.getNames().front(), "people");

    expectImported("people");
}

// Without AS the graph takes the name of the imported directory, which the analyzer fills
// in from the path before codegen ever sees the statement.
TEST_F(LoadParquetV3Test, namesTheGraphAfterTheDirectoryWhenAsIsOmitted) {
    writeFixtureDirectory("typed");

    GraphNameSink sink;
    runQuery("LOAD PARQUET 'typed'", _sessionGraph, sink);

    ASSERT_EQ(sink.getNames().size(), 1u);
    EXPECT_EQ(sink.getNames().front(), "typed");

    expectImported("typed");
}

// A name the session already holds is not imported over, and the graph behind it keeps
// what it had. The failure names the statement the user wrote rather than the shared
// import call all three formats go through.
TEST_F(LoadParquetV3Test, refusesToImportOntoAGraphNameAlreadyLoaded) {
    writeFixtureDirectory("typed");

    GraphNameSink sink;
    runQuery("LOAD PARQUET 'typed' AS people", _sessionGraph, sink);

    runQueryExpectingError("LOAD PARQUET 'typed' AS people",
                           "LOAD PARQUET: failed to import graph 'people'");

    expectImported("people");
}

TEST_F(LoadParquetV3Test, reportsADirectoryThatIsNotThere) {
    runQueryExpectingError("LOAD PARQUET 'absent' AS absent",
                           "LOAD PARQUET: failed to import graph 'absent' from 'absent'");
}

// The importer reads inside the data directory only, so a path that climbs out of it
// imports nothing.
TEST_F(LoadParquetV3Test, readsInsideTheDataDirectoryOnly) {
    writeFixtureDirectory("typed");

    runQueryExpectingError("LOAD PARQUET '../../typed' AS escaped",
                           "LOAD PARQUET: failed to import graph 'escaped'");
}

// A graph name is an identifier, so a directory whose name is not one has to be given a
// name with AS - the check is the analyzer's, ahead of any file access.
TEST_F(LoadParquetV3Test, refusesAGraphNameThatIsNotAnIdentifier) {
    runQueryExpectingError("LOAD PARQUET 'split-export'",
                           "Graph name must only contain alphanumeric characters");
}
