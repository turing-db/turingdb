#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
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

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view ROWS_FILE = "a,b\n1,2\n3,4\n";

// The squared L2 distances from (1, 0) are 0 for node 0, 2 for node 1 and 1 for node 2
constexpr std::string_view VECTOR_FILE = "0,1,0\n1,0,1\n2,1,1\n";

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

}

class SourceParameterTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        writeFile("rows.csv", ROWS_FILE);
        writeFile("vectors.csv", VECTOR_FILE);
    }

protected:
    void writeFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
    }

    void runQuery(std::string_view query, const ParameterMap& parameters, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink,
                              &parameters);
    }

    void createVectorIndex() {
        const ParameterMap noParameters;

        for (const std::string_view command : {"CREATE VECTOR INDEX idx WITH DIMENSION 2 METRIC EUCLID",
                                               "LOAD VECTOR FROM \"vectors.csv\" IN idx"}) {
            QueryStatus status;
            StringRowSink sink;
            runQuery(command, noParameters, status, sink);
            ASSERT_TRUE(status.isOk()) << command << ": " << status.getError();
        }
    }

    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectError(std::string_view query, const ParameterMap& parameters, std::string_view reason) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, status, sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;
        EXPECT_TRUE(contains(status.getError(), reason)) << query << ": " << status.getError();
    }

    void dbProgramOf(std::string_view query, const ParameterMap& parameters, std::string& program) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), parameters, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(SourceParameterTest, loadsTheFileAPathParameterNamesByHeader) {
    ParameterMap parameters;
    parameters.insert("path").setString("rows.csv");

    expectRows("LOAD CSV $path WITH HEADERS AS row RETURN row.a, row.b", parameters, {{"1", "2"}, {"3", "4"}});
}

TEST_F(SourceParameterTest, loadsTheFileAPathParameterNamesByPosition) {
    ParameterMap parameters;
    parameters.insert("path").setString("rows.csv");

    expectRows("LOAD CSV $path AS row RETURN row[0], row[1]", parameters, {{"a", "b"}, {"1", "2"}, {"3", "4"}});
}

TEST_F(SourceParameterTest, loadsTheFileANumberedParameterNames) {
    ParameterMap parameters;
    parameters.insert("0").setString("rows.csv");

    expectRows("LOAD CSV $0 WITH HEADERS AS row RETURN row.b", parameters, {{"2"}, {"4"}});
}

TEST_F(SourceParameterTest, loadsTheFileAPathParameterNamesForEveryMatchedRow) {
    ParameterMap parameters;
    parameters.insert("path").setString("rows.csv");

    expectRows("MATCH (n:Person {name: 'Remy'}) LOAD CSV $path WITH HEADERS AS row RETURN n.name, row.a",
               parameters,
               {{"Remy", "1"}, {"Remy", "3"}});
}

TEST_F(SourceParameterTest, readsThePathParameterWhenTheLoadRuns) {
    ParameterMap parameters;
    parameters.insert("path").setString("rows.csv");

    std::string program;
    dbProgramOf("LOAD CSV $path AS row RETURN row[0]", parameters, program);

    EXPECT_TRUE(contains(program, "db.load_csv(%arg0 : !db.column<!storage.string>")) << program;
    EXPECT_FALSE(contains(program, "rows.csv")) << program;
}

TEST_F(SourceParameterTest, refusesAPathParameterThatIsNotAString) {
    ParameterMap parameters;
    parameters.insert("path").setInt64(1);
    expectError("LOAD CSV $path AS row RETURN row[0]", parameters, "LOAD CSV path must be a string");

    parameters.set("path", ParameterValue());
    expectError("LOAD CSV $path AS row RETURN row[0]", parameters, "LOAD CSV path must be a string");
}

TEST_F(SourceParameterTest, resolvesAPathParameterOutsideTheDataDirectoryAsALiteral) {
    const ParameterMap noParameters;
    expectError("LOAD CSV '../rows.csv' AS row RETURN row[0]", noParameters, "it must be relative to");

    ParameterMap parameters;
    parameters.insert("path").setString("../rows.csv");
    expectError("LOAD CSV $path AS row RETURN row[0]", parameters, "it must be relative to");
}

TEST_F(SourceParameterTest, searchesForTheNeighbourCountAParameterHolds) {
    createVectorIndex();

    const ParameterMap noParameters;
    expectRows("VECTOR SEARCH IN idx FOR 2 (1.0, 0.0) YIELD ids RETURN ids", noParameters, {{"0"}, {"2"}});

    ParameterMap parameters;
    parameters.insert("k").setInt64(2);
    expectRows("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, {{"0"}, {"2"}});

    parameters.insert("k").setInt64(1);
    expectRows("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids, score RETURN ids, score", parameters, {{"0", "0"}});
}

TEST_F(SourceParameterTest, reportsEveryNeighbourForACountBeyondTheIndex) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("k").setInt64(5);

    expectRows("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, {{"0"}, {"2"}, {"1"}});
}

TEST_F(SourceParameterTest, searchesForANeighbourCountParameterForEveryMatchedRow) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("k").setInt64(2);

    expectRows("MATCH (n:Person {name: 'Remy'}) VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN n.name, ids",
               parameters,
               {{"Remy", "0"}, {"Remy", "2"}});
}

TEST_F(SourceParameterTest, readsTheNeighbourCountParameterWhenTheSearchRuns) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(2);

    std::string program;
    dbProgramOf("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, program);

    EXPECT_TRUE(contains(program, "db.vector_search(\"idx\", %arg0 : !db.column<i64>")) << program;
}

TEST_F(SourceParameterTest, refusesANeighbourCountParameterBelowOne) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("k").setInt64(0);
    expectError("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, "k value must be greater than 0");

    parameters.insert("k").setInt64(-1);
    expectError("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, "k value must be greater than 0");
}

TEST_F(SourceParameterTest, refusesANeighbourCountParameterThatIsNotAnInteger) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("k").setDouble(2.5);
    expectError("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, "k value must be an integer");

    parameters.set("k", ParameterValue());
    expectError("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, "k value must be an integer");

    parameters.insert("k").setString("2");
    expectError("VECTOR SEARCH IN idx FOR $k (1.0, 0.0) YIELD ids RETURN ids", parameters, "k value must be an integer");
}
