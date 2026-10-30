#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view KEYWORD_VARIABLES[] = {
    "s3", "S3", "csv", "jsonl", "vector", "change", "graph", "commit", "submit",
};

constexpr std::string_view NEAR_KEYWORD_VARIABLES[] = {
    "s3x", "x_s3", "embedding", "history", "`s3`",
};

constexpr std::pair<std::string_view, int64_t> KEYWORD_PROPERTIES[] = {
    {"graph", 1}, {"vector", 2}, {"s3", 3}, {"commit", 4}, {"change", 5}, {"csv", 6}, {"jsonl", 7},
};

}

class SupplierKeywordTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto addSupplier = [&](std::string_view name) {
            const NodeID supplier = writer.addNode({"Supplier"});
            writer.addNodeProperty<types::String>(supplier, "name", std::string_view {name});
            return supplier;
        };

        const NodeID arden = addSupplier("Arden Systems");
        const NodeID brook = addSupplier("Brook Alloys");
        const NodeID corvel = addSupplier("Corvel Optics");
        const NodeID delta = addSupplier("Delta Castings");
        const NodeID ember = addSupplier("Ember Coatings");

        const NodeID fenwick = writer.addNode({"Supplier", "Graph", "S3", "Vector"});
        writer.addNodeProperty<types::String>(fenwick, "name", std::string_view {"Fenwick Glass"});
        for (const auto& [key, value] : KEYWORD_PROPERTIES) {
            writer.addNodeProperty<types::Int64>(fenwick, key, int64_t {value});
        }

        writer.addEdge("SUB_SUPPLIED_BY", arden, brook);
        writer.addEdge("SUB_SUPPLIED_BY", arden, corvel);
        writer.addEdge("SUB_SUPPLIED_BY", brook, delta);
        writer.addEdge("SUB_SUPPLIED_BY", brook, ember);
        writer.addEdge("SUB_SUPPLIED_BY", corvel, fenwick);

        writer.submit();
        jobSystem.terminate();
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    void expectSupplierCount(std::string_view variable) {
        const std::string name {variable};
        expectSortedRows("MATCH (" + name + ":Supplier) RETURN count(" + name + ")", {{"6"}});
    }

    const std::string _graphName {"suppliers"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(SupplierKeywordTest, readsTheTierThreeSuppliers) {
    expectSortedRows("MATCH (s1:Supplier)-[:SUB_SUPPLIED_BY]->(s2:Supplier)-[:SUB_SUPPLIED_BY]->(s3:Supplier) RETURN s3.name",
                     {{"Delta Castings"}, {"Ember Coatings"}, {"Fenwick Glass"}});
}

TEST_F(SupplierKeywordTest, readsEveryTierOfTheChain) {
    expectSortedRows("MATCH (s1:Supplier)-[:SUB_SUPPLIED_BY]->(s2:Supplier)-[:SUB_SUPPLIED_BY]->(s3:Supplier) RETURN s1.name, s2.name, s3.name",
                     {{"Arden Systems", "Brook Alloys", "Delta Castings"},
                      {"Arden Systems", "Brook Alloys", "Ember Coatings"},
                      {"Arden Systems", "Corvel Optics", "Fenwick Glass"}});
}

TEST_F(SupplierKeywordTest, countsSuppliersNamedAfterAKeyword) {
    for (const std::string_view variable : KEYWORD_VARIABLES) {
        expectSupplierCount(variable);
    }
}

TEST_F(SupplierKeywordTest, countsSuppliersNamedNearAKeyword) {
    for (const std::string_view variable : NEAR_KEYWORD_VARIABLES) {
        expectSupplierCount(variable);
    }
}

TEST_F(SupplierKeywordTest, readsPropertiesNamedAfterKeywords) {
    expectSortedRows("MATCH (n:Supplier {name: 'Fenwick Glass'}) RETURN n.graph, n.vector, n.s3, n.commit, n.change, n.csv, n.jsonl",
                     {{"1", "2", "3", "4", "5", "6", "7"}});
}

TEST_F(SupplierKeywordTest, matchesASupplierByAPropertyNamedAfterAKeyword) {
    for (const auto& [key, value] : KEYWORD_PROPERTIES) {
        const std::string query = "MATCH (n:Supplier {" + std::string {key} + ": " + std::to_string(value) + "}) RETURN n.name";
        expectSortedRows(query, {{"Fenwick Glass"}});
    }
}

TEST_F(SupplierKeywordTest, matchesAnUnlabelledNodeByAPropertyNamedAfterAKeyword) {
    expectSortedRows("MATCH (n {graph: 1}) RETURN n.name", {{"Fenwick Glass"}});
    expectSortedRows("MATCH (n {vector: 2}) RETURN n.name", {{"Fenwick Glass"}});
}

TEST_F(SupplierKeywordTest, keysAMapLiteralWithKeywords) {
    expectSortedRows("WITH {graph: 1, vector: 2, s3: 3} AS m RETURN m.graph, m.vector, m.s3", {{"1", "2", "3"}});
}

TEST_F(SupplierKeywordTest, matchesLabelsNamedAfterKeywords) {
    expectSortedRows("MATCH (n:Graph) RETURN n.name", {{"Fenwick Glass"}});
    expectSortedRows("MATCH (n:S3) RETURN n.name", {{"Fenwick Glass"}});
    expectSortedRows("MATCH (n:Vector) RETURN n.name", {{"Fenwick Glass"}});
}

TEST_F(SupplierKeywordTest, namesAVariableLabelAndPropertyAlike) {
    expectSortedRows("MATCH (s3:S3 {s3: 3}) RETURN s3.s3", {{"3"}});
    expectSortedRows("MATCH (graph:Graph {graph: 1}) RETURN graph.graph", {{"1"}});
}
