#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "IRException.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "NLProgram.h"
#include "NLTranslator.h"

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

// Scan all nodes and output them
constexpr const char* scanProgram = R"mlir(
func.func @scan() {
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
func.func @one_hop_out() {
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
func.func @one_hop_in() {
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
func.func @two_hop() {
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
func.func @cross_loop_output() {
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
func.func @cross_loop_carry() {
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

}

class NLInterpreterTest : public TuringTest {
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

    void runProgram(const char* programText,
                    const GraphView& view,
                    size_t chunkSize,
                    CollectingNodeSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        auto functions = module->getOps<mlir::func::FuncOp>();
        ASSERT_TRUE(functions.begin() != functions.end());

        NLProgram program;
        program.setChunkSize(chunkSize);

        NLTranslator translator(program);
        translator.translate(*functions.begin());

        NLInterpreter::run(view, program, sink);
    }

    // Parses a program that MLIR accepts but the translator must reject
    void expectTranslationFailure(const char* programText) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        auto functions = module->getOps<mlir::func::FuncOp>();
        ASSERT_TRUE(functions.begin() != functions.end());

        NLProgram program;
        NLTranslator translator(program);
        EXPECT_THROW(translator.translate(*functions.begin()), IRException);
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(NLInterpreterTest, emptyGraphProducesNoOutput) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(scanProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    EXPECT_TRUE(sink.getColumns().empty());
}

TEST_F(NLInterpreterTest, scanNodesOutput) {
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

TEST_F(NLInterpreterTest, oneHopOutEdges) {
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

TEST_F(NLInterpreterTest, oneHopInEdges) {
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

TEST_F(NLInterpreterTest, twoHopWithCarriedColumn) {
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

TEST_F(NLInterpreterTest, oneHopSmallChunksTwoParts) {
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

TEST_F(NLInterpreterTest, rejectsCrossLoopOutputColumns) {
    expectTranslationFailure(crossLoopOutputProgram);
}

TEST_F(NLInterpreterTest, rejectsCrossLoopCarriedColumns) {
    expectTranslationFailure(crossLoopCarryProgram);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
