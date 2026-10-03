#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListView.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "SimpleGraph.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The one row a keyless collect drains: the list it gathered and the tally the reduction
// beside it kept.
class ListAndTallySink : public NLOutputSink {
public:
    using Row = std::pair<size_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        const auto* tallies = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(tallies, nullptr);

        for (size_t row = offset; row < offset + rowCount; row++) {
            _rows.emplace_back((*lists)[row].size(), (*tallies)[row]);
        }
    }

    const std::vector<Row>& getRows() const { return _rows; }

private:
    std::vector<Row> _rows;
};

// An ungrouped collect that also counts the rows it gathered: one accumulator holds the
// list and the reduction, and its single group exists whether or not a row arrives.
constexpr const char* keylessCollectAndCount = R"mlir(
func.func @main() {
  %buffer = nl.collect_buffer keys 0 aggregates [count]
  %nodes = nl.scan_nodes()
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.collect_update %buffer, (%node, %node) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
  }
  %groups = nl.collect(%buffer) : !nl.iter<!nl.chunk<!storage.list<!storage.node_id>>, !nl.chunk<ui64>>
  nl.for %list, %total in %groups : !nl.iter<!nl.chunk<!storage.list<!storage.node_id>>, !nl.chunk<ui64>> {
    nl.output(%list, %total) names ["nodes", "total"] : !nl.chunk<!storage.list<!storage.node_id>>, !nl.chunk<ui64>
  }
  func.return
}
)mlir";

// simpledb carries eighteen nodes
constexpr size_t simpledbNodeCount = 18;

}

// collect() over no row is the empty list rather than the absence of a row, so an
// ungrouped collect emits its one group whether or not the match fed it - and the
// reductions taken over the same group have to emit with it.
class KeylessCollectEmptyInputTest : public TuringTest {
protected:
    void runProgram(const char* programText, const GraphView& view, NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        LocalMemory memory;
        NLInterpreter interpreter(*module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();
    }
};

TEST_F(KeylessCollectEmptyInputTest, talliesTheRowsItGathered) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ListAndTallySink sink;
    runProgram(keylessCollectAndCount, reader.getView(), sink);

    const std::vector<ListAndTallySink::Row> expected {{simpledbNodeCount, simpledbNodeCount}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(KeylessCollectEmptyInputTest, emitsAnEmptyListAndAZeroTallyWhenNoRowArrives) {
    auto graph = Graph::create();

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ListAndTallySink sink;
    runProgram(keylessCollectAndCount, reader.getView(), sink);

    const std::vector<ListAndTallySink::Row> expected {{0, 0}};
    EXPECT_EQ(sink.getRows(), expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
