#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "Path.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "comparators/GraphComparator.h"
#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeMap.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view ONE_FLOAT_GRAPH = R"({"type":"node","id":"0","labels":["Vec"],"properties":{"name":"a","v":[0.5]}}
{"type":"node","id":"1","labels":["Vec"],"properties":{"name":"b","v":[-2.0]}}
{"type":"relationship","id":"0","label":"LINK","properties":{"w":[1.5]},"start":0,"end":1}
)";

constexpr std::string_view LOAD_ONE_FLOAT_GRAPH = R"(LOAD JSONL "one.jsonl" AS one WITH EMBEDDINGS [{"v", 1}, {"w", 1}])";

void setEmbedding(ParameterMap& parameters, std::string_view name, const std::vector<float>& floats) {
    parameters.insert(name).setEmbedding(floats);
}

}

// An embedding holds at least 1 float, as in pgvector, Neo4j's vector index and Qdrant.
class SmallestEmbeddingDimensionTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void writeDataFile(std::string_view name, std::string_view contents) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        ASSERT_TRUE(file.is_open());

        file << contents;
    }

    void runQuery(std::string_view query, std::string_view graphName, const ParameterMap& parameters, QueryStatus& status, StringRowSink& sink) {
        _interpreter->execute(status, query, graphName, CommitHash::head(), ChangeID::head(), &sink, &parameters);
    }

    void run(std::string_view query) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, _graphName, {}, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    void expectRows(std::string_view query, std::string_view graphName, const ParameterMap& parameters, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, graphName, parameters, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectSortedRows(std::string_view query, std::string_view graphName, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, graphName, {}, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        Rows rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << query;
    }

    void expectAnalyzeError(std::string_view query, std::string_view reason) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, _graphName, {}, status, sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;
        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << query << ": " << status.getError();
        EXPECT_NE(status.getError().find(reason), std::string::npos) << query << ": " << status.getError();
    }

    void writeOneFloatVectors() {
        JobSystem jobSystem;
        jobSystem.init();

        const std::vector<float> a {0.5f};
        const std::vector<float> b {-2.0f};

        GraphWriter writer(_graph, &jobSystem);

        const NodeID first = writer.addNode({"Vec"});
        writer.addNodeProperty<types::String>(first, "name", std::string_view {"a"});
        writer.addNodeProperty<types::Embedding>(first, "v", std::span<const float> {a});

        const NodeID second = writer.addNode({"Vec"});
        writer.addNodeProperty<types::String>(second, "name", std::string_view {"b"});
        writer.addNodeProperty<types::Embedding>(second, "v", std::span<const float> {b});

        ASSERT_TRUE(writer.submit());

        jobSystem.terminate();
    }

    void searchAnIndexOfOneDimension(std::string_view indexName, std::string_view indexType) {
        const NodeID remy = SimpleGraph::findNodeID(_graph, "Remy");
        const NodeID adam = SimpleGraph::findNodeID(_graph, "Adam");
        const NodeID computers = SimpleGraph::findNodeID(_graph, "Computers");

        const std::string file = fmt::format("{}.csv", indexName);
        writeDataFile(file, fmt::format("{},0\n{},1\n{},5\n", remy.getValue(), adam.getValue(), computers.getValue()));

        run(fmt::format("CREATE VECTOR INDEX {} WITH DIMENSION 1 METRIC EUCLID TYPE {}", indexName, indexType));
        run(fmt::format(R"(LOAD VECTOR FROM "{}" IN {})", file, indexName));
        expectRows("SHOW VECTOR INDEXES", _graphName, {}, {{std::string {indexName}, "1"}});

        ParameterMap parameters;
        setEmbedding(parameters, "q", {1.0f});

        expectRows(fmt::format("VECTOR SEARCH IN {} FOR 2 $q YIELD ids, score RETURN ids, score", indexName),
                   _graphName,
                   parameters,
                   {{std::to_string(adam.getValue()), "0"}, {std::to_string(remy.getValue()), "1"}});
    }

    const std::string _graphName {"simpledb"};
    Graph* _graph {nullptr};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(SmallestEmbeddingDimensionTest, loadsAJsonlEmbeddingOfOneFloat) {
    writeDataFile("one.jsonl", ONE_FLOAT_GRAPH);
    run(LOAD_ONE_FLOAT_GRAPH);

    expectSortedRows("MATCH (n:Vec) RETURN n.name, n.v", "one", {{"a", "0.5"}, {"b", "-2"}});
    expectSortedRows("MATCH ()-[e:LINK]->() RETURN e.w", "one", {{"1.5"}});
    expectSortedRows("CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
                     "one",
                     {{"name", "String"}, {"v", "Embedding"}, {"w", "Embedding"}});
}

TEST_F(SmallestEmbeddingDimensionTest, refusesAJsonlEmbeddingOfNoFloat) {
    writeDataFile("none.jsonl", R"({"type":"node","id":"0","labels":["Vec"],"properties":{"v":[]}}
)");

    expectAnalyzeError(R"(LOAD JSONL "none.jsonl" AS none WITH EMBEDDINGS [{"v", 0}])",
                       "Embedding property 'v' must have dimension greater than 0");
    expectAnalyzeError(R"(LOAD JSONL "none.jsonl" AS none WITH EMBEDDINGS [{"w", 1}, {"v", 0}])",
                       "Embedding property 'v' must have dimension greater than 0");
}

TEST_F(SmallestEmbeddingDimensionTest, writesAnEmbeddingOfOneFloatThroughTheGraphWriter) {
    writeOneFloatVectors();

    expectSortedRows("MATCH (n:Vec) RETURN n.name, n.v", _graphName, {{"a", "0.5"}, {"b", "-2"}});
}

TEST_F(SmallestEmbeddingDimensionTest, scoresEmbeddingsOfOneFloat) {
    writeOneFloatVectors();

    ParameterMap parameters;
    setEmbedding(parameters, "q", {4.0f});

    expectRows("MATCH (a:Vec {name: 'a'}), (b:Vec {name: 'b'}) "
               "RETURN cosine_similarity(a.v, b.v), euclidean_distance(a.v, b.v), cosine_similarity(a.v, $q), euclidean_distance(b.v, $q)",
               _graphName,
               parameters,
               {{"-1", "2.5", "1", "6"}});
}

TEST_F(SmallestEmbeddingDimensionTest, dumpsAndLoadsAnEmbeddingOfOneFloat) {
    writeOneFloatVectors();

    const fs::Path dumpPath = fs::Path {_outDir} / "dump";
    const auto dumped = GraphDumper::dump(_graph, dumpPath);
    ASSERT_TRUE(dumped) << dumped.error().fmtMessage();

    const std::unique_ptr<Graph> loaded = Graph::create();
    const auto read = GraphLoader::load(loaded.get(), dumpPath);
    ASSERT_TRUE(read) << read.error().fmtMessage();

    EXPECT_TRUE(GraphComparator::same(*_graph, *loaded));

    const FrozenCommitTx transaction = loaded->openTransaction();
    const GraphView view = transaction.viewGraph();
    const PropertyTypeMap& propertyTypes = view.metadata().propTypes();

    const std::optional<PropertyType> v = propertyTypes.get("v");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(propertyTypes.getEmbeddingDimension(v->_id), 1u);
}

TEST_F(SmallestEmbeddingDimensionTest, searchesAFlatIndexOfOneDimension) {
    searchAnIndexOfOneDimension("flat_line", "FLAT");
}

TEST_F(SmallestEmbeddingDimensionTest, searchesAnHNSWIndexOfOneDimension) {
    searchAnIndexOfOneDimension("hnsw_line", "HNSW");
}

TEST_F(SmallestEmbeddingDimensionTest, refusesAVectorIndexOfNoDimension) {
    expectAnalyzeError("CREATE VECTOR INDEX none WITH DIMENSION 0 METRIC EUCLID",
                       "Vector index dimension must be greater than 0");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
