#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
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

    const std::vector<std::vector<uint64_t>>& getColumns() const { return _columns; }

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

// Collects (node ID, nullable int64 property) rows, for programs that read an
// Int64 property: a node ID chunk and a !nl.nullable<i64> value chunk.
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

// Scan all nodes and output them
constexpr const char* scanProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    nl.output(%a) : !nl.chunk<!nl.node_id>
  }
  func.return
}
)mlir";

// One hop along out-edges, outputting (source, target) pairs. The source
// column exercises the gather-by-indices reconstruction.
constexpr const char* oneHopOutProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
      nl.output(%srcs, %b) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>
    }
  }
  func.return
}
)mlir";

// One hop along in-edges: the writer fills the source side and the target
// side is gathered from the input, so the output pairs are the same edge set
constexpr const char* oneHopInProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    %edges = nl.get_in_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
      nl.output(%srcs, %b) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>
    }
  }
  func.return
}
)mlir";

// Two hops a->b->c carrying a through the second hop, outputting (a, c) pairs
constexpr const char* twoHopProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
      %hop = nl.get_out_edges(%b, {%srcs}) : !nl.chunk<!nl.node_id>
      nl.for %srcs2, %eids2, %etypes2, %c, %aCarried in %hop : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>> {
        nl.output(%aCarried, %c) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>
      }
    }
  }
  func.return
}
)mlir";

// Verifier-legal but rejected by the translator: outputs an outer loop
// variable from the inner loop instead of carrying it through the carry set
constexpr const char* crossLoopOutputProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
      nl.output(%a, %b) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>
    }
  }
  func.return
}
)mlir";

// Verifier-legal but rejected by the translator: carries a chunk bound by a
// different loop than the one binding the input chunk
constexpr const char* crossLoopCarryProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
      %hop = nl.get_out_edges(%b, {%a}) : !nl.chunk<!nl.node_id>
      nl.for %srcs2, %eids2, %etypes2, %c, %aCarried in %hop : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>> {
        nl.output(%aCarried, %c) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>
      }
    }
  }
  func.return
}
)mlir";

// Resolve the "score" property once above the loops, then read it per scanned
// node and output (node, score). The value chunk is nullable, so nodes without
// the property still appear, with a null value.
constexpr const char* nodePropertiesProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
    %values = nl.get_node_properties(%a, %score) : !nl.chunk<!nl.nullable<i64>>
    nl.output(%a, %values) : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.nullable<i64>>
  }
  func.return
}
)mlir";

}

class NLExecutorTest : public TuringTest {
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

    // A second datapart with nodes 4, 5 and the edge 4 -> 5, exercising the
    // datapart-major iteration of the writers
    void addSecondPart(Graph& graph) {
        auto change = graph.newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeE = builder.addNode(labelset);
        const NodeID nodeF = builder.addNode(labelset);

        builder.addEdge(0, nodeE, nodeF);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);
    }

    // A line graph 0 -> 1 -> 2 where nodes 0 and 1 carry a "score" Int64
    // property (100, 200) and node 2 has none, so a property read yields null
    std::unique_ptr<Graph> buildScoredGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");
        const PropertyType scoreType = metadata.getOrCreatePropertyType("score", ValueType::Int64);
        const PropertyTypeID scoreID = scoreType._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);

        builder.addEdge(0, nodeA, nodeB);
        builder.addEdge(0, nodeB, nodeC);

        builder.addNodeProperty<types::Int64>(nodeA, scoreID, 100);
        builder.addNodeProperty<types::Int64>(nodeB, scoreID, 200);
        // nodeC has no "score" property, so a property read returns null for it

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    void runProgram(const char* programText,
                    const GraphView& view,
                    size_t chunkSize,
                    NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        LocalMemory memory;
        NLInterpreter interpreter(*module, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    // Parses a program that MLIR accepts but the translator must reject
    void expectTranslationFailure(const char* programText) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        // run() reaches the translator before touching the graph view or sink,
        // so a rejected program surfaces its IRException with neither supplied
        LocalMemory memory;
        NLInterpreter interpreter(*module, nullptr, nullptr, &memory);
        EXPECT_THROW(interpreter.run(), IRException);
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(NLExecutorTest, emptyGraphProducesNoOutput) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(scanProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    EXPECT_TRUE(sink.getColumns().empty());
}

TEST_F(NLExecutorTest, scanNodesOutput) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(scanProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<uint64_t>> expected {{0}, {1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, getNodeProperties) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runProgram(nodePropertiesProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Every node appears, with its score or null where it has none
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, oneHopOutEdges) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(oneHopOutProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, oneHopInEdges) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(oneHopInProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, twoHopWithCarriedColumn) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(twoHopProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Both two-hop paths go from 0 to 3, one through 1 and one through 2
    const std::vector<std::vector<uint64_t>> expected {{0, 3}, {0, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, oneHopSmallChunksTwoParts) {
    auto graph = buildDiamondGraph();
    addSecondPart(*graph);

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A chunk size smaller than the node count forces several steps per loop;
    // the result must not depend on the chunking
    CollectingNodeSink sink;
    runProgram(oneHopOutProgram, reader.getView(), 2, sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}, {4, 5}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, rejectsCrossLoopOutputColumns) {
    expectTranslationFailure(crossLoopOutputProgram);
}

TEST_F(NLExecutorTest, rejectsCrossLoopCarriedColumns) {
    expectTranslationFailure(crossLoopCarryProgram);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
