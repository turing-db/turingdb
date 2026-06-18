#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "IRException.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects every output chunk into one accumulated value vector per column.
// All test programs output node ID chunks only.
class CollectingNodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks) override {
        if (_columns.empty()) {
            _columns.resize(chunks.size());
        }

        ASSERT_EQ(chunks.size(), _columns.size());

        for (size_t columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
            const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[columnIndex]);
            ASSERT_NE(nodeIDs, nullptr);

            for (const NodeID nodeID : *nodeIDs) {
                _columns[columnIndex].push_back(nodeID.getValue());
            }
        }
    }

    // Fills rows zipped from the columns and sorted: chunk order depends on
    // datapart-major iteration, so tests compare order-independently
    void sortedRows(std::vector<std::vector<uint64_t>>& rows) const {
        rows.clear();
        const size_t rowCount = _columns.empty() ? 0 : _columns.front().size();

        for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            std::vector<uint64_t> row;
            for (const std::vector<uint64_t>& column : _columns) {
                row.push_back(column[rowIndex]);
            }
            rows.push_back(row);
        }

        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::vector<uint64_t>> _columns;
};

// Collects (node ID, nullable int64 property) rows. Output programs that read
// an Int64 property emit a node ID chunk and a !nl.nullable<i64> value chunk.
class CollectingNodeIntPropSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(nodeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodeIDs->size(), values->size());

        const auto& idRaw = nodeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = 0; rowIndex < nodeIDs->size(); rowIndex++) {
            _rows.push_back({idRaw[rowIndex].getValue(), valueRaw[rowIndex]});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<int64_t>>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> _rows;
};

// Collects (node ID, nullable string property) rows. The value column is a
// !nl.nullable<!nl.string> chunk, storage's ColumnOptVector<string_view>.
class CollectingNodeStringPropSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[1]);
        ASSERT_NE(nodeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodeIDs->size(), values->size());

        const auto& idRaw = nodeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = 0; rowIndex < nodeIDs->size(); rowIndex++) {
            std::optional<std::string> value;
            if (valueRaw[rowIndex]) {
                value = std::string(*valueRaw[rowIndex]);
            }
            _rows.push_back({idRaw[rowIndex].getValue(), value});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<std::string>>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::pair<uint64_t, std::optional<std::string>>> _rows;
};

// Collects (node ID, nullable embedding property) rows. The value column is a
// !nl.nullable<!nl.embedding> chunk, storage's ColumnOptVector<span<float>>;
// each span is copied out since it points into the live graph storage.
class CollectingNodeEmbeddingPropSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<std::span<const float>>*>(chunks[1]);
        ASSERT_NE(nodeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodeIDs->size(), values->size());

        const auto& idRaw = nodeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = 0; rowIndex < nodeIDs->size(); rowIndex++) {
            std::optional<std::vector<float>> value;
            if (valueRaw[rowIndex]) {
                value = std::vector<float>(valueRaw[rowIndex]->begin(), valueRaw[rowIndex]->end());
            }
            _rows.push_back({idRaw[rowIndex].getValue(), value});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>> _rows;
};

// Scan all nodes and output them
constexpr const char* scanProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  db.output(%a) : !db.column<"a">
  return
}
)mlir";

// One hop along out-edges, outputting (source, target) pairs
constexpr const char* oneHopOutProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<"a">) -> (!db.column<"srcs">, !db.column<"eids">, !db.column<"etypes">, !db.column<"b">)
  db.output(%srcs, %b) : !db.column<"srcs">, !db.column<"b">
  return
}
)mlir";

// Two hops a->b->c carrying a through the second hop, outputting (a, c) pairs
constexpr const char* twoHopProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<"a">) -> (!db.column<"a1">, !db.column<"e0">, !db.column<"et0">, !db.column<"b">)
  %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<"b">, !db.column<"a1">) -> (!db.column<"b2">, !db.column<"e1">, !db.column<"et1">, !db.column<"c">, !db.column<"a2">)
  db.output(%a2, %c) : !db.column<"a2">, !db.column<"c">
  return
}
)mlir";

// Scan all nodes and output each node with its "score" property, which some
// nodes lack (those come back null, none are dropped)
constexpr const char* nodePropertiesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  %score = db.get_node_properties(%a, "score") : (!db.column<"a">) -> !db.column<"a.score">
  db.output(%a, %score) : !db.column<"a">, !db.column<"a.score">
  return
}
)mlir";

// Read the string "name" property of each node
constexpr const char* nodeNameProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  %name = db.get_node_properties(%a, "name") : (!db.column<"a">) -> !db.column<"a.name">
  db.output(%a, %name) : !db.column<"a">, !db.column<"a.name">
  return
}
)mlir";

// Read the embedding "vec" property of each node
constexpr const char* nodeVecProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<"a">
  %vec = db.get_node_properties(%a, "vec") : (!db.column<"a">) -> !db.column<"a.vec">
  db.output(%a, %vec) : !db.column<"a">, !db.column<"a.vec">
  return
}
)mlir";

}

class DBLoweringTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // A diamond: 0 -> {1, 2} -> 3, so the two-hop pair (0, 3) exists twice
    std::unique_ptr<Graph> buildDiamondGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);
        const NodeID nodeD = builder.addNode(labelset);

        builder.addEdge(0, nodeA, nodeB);
        builder.addEdge(0, nodeA, nodeC);
        builder.addEdge(0, nodeB, nodeD);
        builder.addEdge(0, nodeC, nodeD);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // A line graph 0 -> 1 -> 2 where nodes 0 and 1 carry "score" (Int64),
    // "name" (String) and "vec" (Embedding) properties and node 2 carries none,
    // so a read of any of them returns null for node 2
    std::unique_ptr<Graph> buildPropertyGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;
        const PropertyTypeID vecID = metadata.getOrCreatePropertyType("vec", ValueType::Embedding)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);

        builder.addEdge(0, nodeA, nodeB);
        builder.addEdge(0, nodeB, nodeC);

        builder.addNodeProperty<types::Int64>(nodeA, scoreID, 100);
        builder.addNodeProperty<types::Int64>(nodeB, scoreID, 200);

        builder.addNodeProperty<types::String>(nodeA, nameID, "alice");
        builder.addNodeProperty<types::String>(nodeB, nameID, "bob");

        const std::vector<float> vecA {1.0f, 2.0f};
        const std::vector<float> vecB {3.0f, 4.0f};
        builder.addNodeProperty<types::Embedding>(nodeA, vecID, vecA);
        builder.addNodeProperty<types::Embedding>(nodeB, vecID, vecB);

        // nodeC carries no property, so every property read returns null for it

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Parses a db-dialect program, lowers it to nl with DBLowering, and runs
    // the lowered nl function against the graph view
    void runLoweredProgram(const char* programText,
                           const GraphView& view,
                           NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        // Collect the lowered nl function in its own module, then interpret it
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory);
        interpreter.run();
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(DBLoweringTest, lowersScanNodes) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(scanProgram, reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{0}, {1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersOneHopOutEdges) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(oneHopOutProgram, reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersTwoHopWithCarriedColumn) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(twoHopProgram, reader.getView(), sink);

    // Both two-hop paths go from 0 to 3, one through 1 and one through 2
    const std::vector<std::vector<uint64_t>> expected {{0, 3}, {0, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersGetNodeProperties) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(nodePropertiesProgram, reader.getView(), sink);

    // Every node appears, with its score or null where it has none
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersGetNodeStringProperty) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeStringPropSink sink;
    runLoweredProgram(nodeNameProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<std::string>>> expected {
        {0, std::string("alice")}, {1, std::string("bob")}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<std::string>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersGetNodeEmbeddingProperty) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeEmbeddingPropSink sink;
    runLoweredProgram(nodeVecProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>> expected {
        {0, std::vector<float>{1.0f, 2.0f}}, {1, std::vector<float>{3.0f, 4.0f}}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
