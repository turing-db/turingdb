#include <gtest/gtest.h>

#include <stddef.h>
#include <algorithm>
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
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Two people to import, with a property of each kind a conversion produces.
constexpr std::string_view nodeFile = "Ada,36,1.5\n"
                                      "Grace,45,2.5\n";

constexpr std::string_view headedNodeFile = "name,age\n"
                                            "Ada,36\n"
                                            "Grace,45\n";

// Edges between people simpledb already carries, which is the import a MATCH answers.
constexpr std::string_view edgeFile = "Remy,Adam,2020\n"
                                      "Adam,Remy,2021\n";

// More records than one chunk holds, so a write from a loaded file spans several steps of
// the load's loop.
constexpr size_t chunkedRecordCount = 70000;

}

// What a LOAD CSV reads is rows the rest of the query may write from: a CREATE runs once
// per record, and a MATCH between the two turns a file of names into edges of the graph.
class LoadCSVWriteTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        writeFile("nodes.csv", nodeFile);
        writeFile("headed_nodes.csv", headedNodeFile);
        writeFile("edges.csv", edgeFile);
    }

protected:
    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
        file.close();
    }

    void runQuery(std::string_view query,
                  QueryStatus& status,
                  NLOutputSink& sink,
                  ChangeID change = ChangeID::head()) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              change,
                              &_env->getMem(),
                              &sink);
    }

    ChangeID openChange() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto opened = system.newChange(_graphName);
        bioassert(opened, "Failed to open a change");

        return opened.value()->id();
    }

    void submitChange(ChangeID change) {
        QueryCallbacks callbacks;
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     change);

        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << status.getError();
    }

    // Runs a write inside a change and submits it, checking on the way that a query
    // ending in an update reports no row of its own.
    void runWrite(std::string_view query) {
        const ChangeID change = openChange();

        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink, change);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_TRUE(sink.getRows().empty()) << query << " reported rows";

        submitChange(change);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::ranges::sort(sortedExpected);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected) << query;
    }

    const std::string _graphName = "simpledb";
    QueryConfig _queryConfig;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LoadCSVWriteTest, createsANodePerRecord) {
    runWrite("LOAD CSV 'nodes.csv' AS row "
             "CREATE (n:Imported {name: row[0], age: toInteger(row[1]), score: toFloat(row[2])})");

    expectRows("MATCH (n:Imported) RETURN n.name, n.age, n.score",
               {{"Ada", "36", "1.5"}, {"Grace", "45", "2.5"}});
}

TEST_F(LoadCSVWriteTest, createsANodePerRecordOfAHeadedFile) {
    runWrite("LOAD CSV 'headed_nodes.csv' WITH HEADERS AS row "
             "CREATE (n:Imported {name: row.name, age: toInteger(row.age)})");

    expectRows("MATCH (n:Imported) RETURN n.name, n.age", {{"Ada", "36"}, {"Grace", "45"}});
}

// A query naming no field still writes once per record, since the records are the rows.
TEST_F(LoadCSVWriteTest, createsANodePerRecordWithoutNamingAField) {
    runWrite("LOAD CSV 'nodes.csv' AS row CREATE (n:Marker)");

    expectRows("MATCH (n:Marker) RETURN count(n)", {{"2"}});
}

// The import a file of names asks for: the MATCH resolves both ends against the graph and
// the CREATE writes one edge per record.
TEST_F(LoadCSVWriteTest, createsAnEdgePerRecordBetweenMatchedNodes) {
    runWrite("LOAD CSV 'edges.csv' AS row "
             "MATCH (a {name: row[0]}), (b {name: row[1]}) "
             "CREATE (a)-[:IMPORTED {since: toInteger(row[2])}]->(b)");

    expectRows("MATCH (a)-[e:IMPORTED]->(b) RETURN a.name, b.name, e.since",
               {{"Remy", "Adam", "2020"}, {"Adam", "Remy", "2021"}});
}

TEST_F(LoadCSVWriteTest, setsAPropertyOfEveryMatchedRecord) {
    runWrite("LOAD CSV 'edges.csv' AS row MATCH (a {name: row[0]}) SET a.age = toInteger(row[2])");

    expectRows("MATCH (n:Person) WHERE n.age > 100 RETURN n.name, n.age",
               {{"Remy", "2020"}, {"Adam", "2021"}});
}

TEST_F(LoadCSVWriteTest, deletesEveryMatchedRecord) {
    runWrite("LOAD CSV 'edges.csv' AS row MATCH (a {name: row[0]}) DETACH DELETE a");

    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Adam' RETURN n.name", {});
}

// The write runs once per record however many chunks the file arrives in.
TEST_F(LoadCSVWriteTest, createsANodePerRecordOfAFileSpanningSeveralChunks) {
    std::string content;
    for (size_t record = 0; record < chunkedRecordCount; record++) {
        content += std::to_string(record);
        content += '\n';
    }

    writeFile("chunked.csv", content);

    runWrite("LOAD CSV 'chunked.csv' AS row CREATE (n:Bulk {name: row[0]})");

    expectRows("MATCH (n:Bulk) RETURN count(n)", {{std::to_string(chunkedRecordCount)}});
}
