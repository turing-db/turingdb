#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using Tuple = std::vector<uint64_t>;

// Collects the rows a program emits as tuples of raw IDs, from node and edge ID columns,
// with the row count of every chunk they arrived in
class IDTupleSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _chunkRowCounts.push_back(rowCount);

        for (size_t row = offset; row < offset + rowCount; row++) {
            Tuple tuple;
            for (const Column* chunk : chunks) {
                if (const ColumnEdgeIDs* edges = dynamic_cast<const ColumnEdgeIDs*>(chunk)) {
                    tuple.push_back(edges->getRaw()[row].getValue());
                } else {
                    const ColumnNodeIDs* nodes = dynamic_cast<const ColumnNodeIDs*>(chunk);
                    ASSERT_NE(nodes, nullptr);
                    tuple.push_back(nodes->getRaw()[row].getValue());
                }
            }

            _tuples.push_back(tuple);
        }
    }

    const std::vector<Tuple>& getTuples() const { return _tuples; }

    size_t getLargestChunkRowCount() const {
        if (_chunkRowCounts.empty()) {
            return 0;
        }

        return *std::max_element(_chunkRowCounts.begin(), _chunkRowCounts.end());
    }

private:
    std::vector<Tuple> _tuples;
    std::vector<size_t> _chunkRowCounts;
};

constexpr const char* edgeScanProgram = R"mlir(
func.func @main() {
  %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
  db.output(%e, %s, %d) : !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* nodeScanProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  db.output(%n) : !db.column<!storage.node_id>
  return
}
)mlir";

// Every pair of edges but an edge with itself: the left edge and its source, the right
// edge's target and the right edge
constexpr const char* edgePairProgram = R"mlir(
func.func @main() {
  %0:4 = db.cross_product factor {
    %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
    db.yield %e, %s : !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  } factor {
    %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
    db.yield %d, %e : !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } distinct_from [0, 1]
  db.output(%0#0, %0#1, %0#2, %0#3) : !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  return
}
)mlir";

constexpr const char* limitedEdgePairProgram = R"mlir(
func.func @main() {
  %0:4 = db.cross_product factor {
    %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
    db.yield %e, %s : !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  } factor {
    %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
    db.yield %d, %e : !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  } distinct_from [0, 1]
  %1:4 = db.limit(%0#0, %0#1, %0#2, %0#3) count 40 : (!db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>) -> (!db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>)
  db.output(%1#0, %1#1, %1#2, %1#3) : !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  return
}
)mlir";

// The right side is itself a product, of the edges with the nodes, so each right edge
// comes out once per node
constexpr const char* edgePairNodeProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
    db.yield %e : !db.column<!storage.edge_id>
  } factor {
    %1:2 = db.cross_product factor {
      %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
      db.yield %e : !db.column<!storage.edge_id>
    } factor {
      %n = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %n : !db.column<!storage.node_id>
    }
    db.yield %1#0, %1#1 : !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  } distinct_from [0, 0]
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.edge_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Two left edges, which may be one edge, against a right edge that differs from both
constexpr const char* twoPairsProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %1:2 = db.cross_product factor {
      %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
      db.yield %e : !db.column<!storage.edge_id>
    } factor {
      %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
      db.yield %e : !db.column<!storage.edge_id>
    }
    db.yield %1#0, %1#1 : !db.column<!storage.edge_id>, !db.column<!storage.edge_id>
  } factor {
    %s, %e, %t, %d = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>
    db.yield %e : !db.column<!storage.edge_id>
  } distinct_from [0, 0, 1, 0]
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.edge_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_id>
  return
}
)mlir";

void runLoweredProgram(const char* programText, const GraphView& view, NLOutputSink& sink, size_t chunkSize) {
    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &view);
    lowering.lower(dbFunction, *nlModule);

    LocalMemory memory;
    NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
    interpreter.run();
}

void sorted(std::vector<Tuple> tuples, std::vector<Tuple>& result) {
    std::sort(tuples.begin(), tuples.end());
    result = std::move(tuples);
}

}

// A cross product with distinct_from leaves out the pairs whose two edges are one edge,
// whatever chunk size cuts the product, and never lays out a chunk past that size.
class CrossProductDistinctEdgesTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    // The (edge, source, target) of every edge and the ID of every node of simpledb
    void readGraph(const GraphView& view, std::vector<Tuple>& edges, std::vector<uint64_t>& nodes) {
        IDTupleSink edgeSink;
        runLoweredProgram(edgeScanProgram, view, edgeSink, ChunkConfig::CHUNK_SIZE);
        edges = edgeSink.getTuples();

        IDTupleSink nodeSink;
        runLoweredProgram(nodeScanProgram, view, nodeSink, ChunkConfig::CHUNK_SIZE);
        nodes.clear();
        for (const Tuple& node : nodeSink.getTuples()) {
            nodes.push_back(node[0]);
        }

        ASSERT_EQ(edges.size(), 18u);
        ASSERT_EQ(nodes.size(), 18u);
    }

    void expectRowsAtEveryChunkSize(const char* programText, const GraphView& view, const std::vector<Tuple>& expected) {
        std::vector<Tuple> expectedSorted;
        sorted(expected, expectedSorted);

        for (const size_t chunkSize : _chunkSizes) {
            IDTupleSink sink;
            runLoweredProgram(programText, view, sink, chunkSize);

            std::vector<Tuple> rows;
            sorted(sink.getTuples(), rows);
            EXPECT_EQ(rows, expectedSorted) << "at chunk size " << chunkSize;
            EXPECT_LE(sink.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;
        }
    }

    const std::vector<size_t> _chunkSizes {1, 2, 3, 4, 7, 17, 18, 19, 20, 100, ChunkConfig::CHUNK_SIZE};

    std::unique_ptr<Graph> _graph;
};

TEST_F(CrossProductDistinctEdgesTest, leavesOutThePairsOfOneEdge) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    std::vector<Tuple> edges;
    std::vector<uint64_t> nodes;
    readGraph(view, edges, nodes);

    std::vector<Tuple> expected;
    for (const Tuple& left : edges) {
        for (const Tuple& right : edges) {
            if (left[0] != right[0]) {
                expected.push_back({left[0], left[1], right[2], right[0]});
            }
        }
    }
    ASSERT_EQ(expected.size(), 306u);

    expectRowsAtEveryChunkSize(edgePairProgram, view, expected);
}

TEST_F(CrossProductDistinctEdgesTest, leavesOutThePairsOfOneEdgeAcrossANestedProduct) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    std::vector<Tuple> edges;
    std::vector<uint64_t> nodes;
    readGraph(view, edges, nodes);

    std::vector<Tuple> expected;
    for (const Tuple& left : edges) {
        for (const Tuple& right : edges) {
            for (const uint64_t node : nodes) {
                if (left[0] != right[0]) {
                    expected.push_back({left[0], right[0], node});
                }
            }
        }
    }
    ASSERT_EQ(expected.size(), 5508u);

    expectRowsAtEveryChunkSize(edgePairNodeProgram, view, expected);
}

TEST_F(CrossProductDistinctEdgesTest, leavesOutARightEdgeEqualToEitherLeftEdge) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    std::vector<Tuple> edges;
    std::vector<uint64_t> nodes;
    readGraph(view, edges, nodes);

    std::vector<Tuple> expected;
    for (const Tuple& first : edges) {
        for (const Tuple& second : edges) {
            for (const Tuple& right : edges) {
                if (right[0] != first[0] && right[0] != second[0]) {
                    expected.push_back({first[0], second[0], right[0]});
                }
            }
        }
    }
    ASSERT_EQ(expected.size(), 5202u);

    expectRowsAtEveryChunkSize(twoPairsProgram, view, expected);
}

TEST_F(CrossProductDistinctEdgesTest, aLimitKeepsTheFirstRowsTheProductLeaves) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();

    for (const size_t chunkSize : _chunkSizes) {
        IDTupleSink unlimited;
        runLoweredProgram(edgePairProgram, view, unlimited, chunkSize);
        ASSERT_EQ(unlimited.getTuples().size(), 306u) << "at chunk size " << chunkSize;

        const std::vector<Tuple> expected(unlimited.getTuples().begin(), unlimited.getTuples().begin() + 40);

        IDTupleSink limited;
        runLoweredProgram(limitedEdgePairProgram, view, limited, chunkSize);
        EXPECT_EQ(limited.getTuples(), expected) << "at chunk size " << chunkSize;
        EXPECT_LE(limited.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;
    }
}
