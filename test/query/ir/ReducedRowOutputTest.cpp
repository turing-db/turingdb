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

#include "IRException.h"
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

// The malformed program below is turned away before it runs, so nothing reaches a sink.
class DiscardingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
    }
};

// The rows of the keyless collect drain: the one list the whole match collapsed to, and
// the tally beside it. Only the sizes matter here, so the list cells are counted rather
// than read.
class ListAndCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(counts, nullptr);

        for (size_t row = offset; row < offset + rowCount; row++) {
            _listSizes.push_back((*lists)[row].size());
            _counts.push_back((*counts)[row]);
        }
    }

    const std::vector<size_t>& getListSizes() const { return _listSizes; }
    const std::vector<uint64_t>& getCounts() const { return _counts; }

private:
    std::vector<size_t> _listSizes;
    std::vector<uint64_t> _counts;
};

// What a keyless collect beside a count lowers to: the drain loop reads the tally the
// function-scope nl.count_result holds, and its one group is its one row - so the tally
// stands for exactly the row that reads it.
constexpr const char* collectDrainReadsTheTally = R"mlir(
func.func @main() {
  %tally = nl.count
  %buffer = nl.collect_buffer keys 0
  %people = nl.scan_nodes_by_label(["Person"])
  nl.for %person in %people : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.collect_update %buffer, (%person) : !nl.chunk<!storage.node_id>
    nl.count_update %tally, %person all_rows : !nl.chunk<!storage.node_id>
  }
  %count = nl.count_result(%tally) : !nl.chunk<ui64>
  %groups = nl.collect(%buffer) : !nl.iter<!nl.chunk<!storage.list<!storage.node_id>>>
  nl.for %list in %groups : !nl.iter<!nl.chunk<!storage.list<!storage.node_id>>> {
    nl.output(%list, %count) names ["people", "total"] : !nl.chunk<!storage.list<!storage.node_id>>, !nl.chunk<ui64>
  }
  func.return
}
)mlir";

// The same tally read by a scan loop instead, which keeps one row per node: nothing
// repeats the single row the reduction holds across the many rows of the step, so the
// tally has no value to give rows past its first.
constexpr const char* scanLoopReadsTheTally = R"mlir(
func.func @main() {
  %tally = nl.count
  %counted = nl.scan_nodes_by_label(["Person"])
  nl.for %person in %counted : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.count_update %tally, %person all_rows : !nl.chunk<!storage.node_id>
  }
  %count = nl.count_result(%tally) : !nl.chunk<ui64>
  %people = nl.scan_nodes_by_label(["Person"])
  nl.for %person in %people : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%person, %count) names ["person", "total"] : !nl.chunk<!storage.node_id>, !nl.chunk<ui64>
  }
  func.return
}
)mlir";

// simpledb carries eight Person nodes
constexpr size_t personCount = 8;

}

// A chunk holding the single row a whole-relation reduction collapsed to, read by an
// nl.output in another block: legal where the step that reads it is that one row, and
// malformed where the step keeps many, since nothing broadcasts it across them.
class ReducedRowOutputTest : public TuringTest {
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

TEST_F(ReducedRowOutputTest, emitsTheTallyBesideTheKeylessCollectItGroupsWith) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ListAndCountSink sink;
    runProgram(collectDrainReadsTheTally, reader.getView(), sink);

    EXPECT_EQ(sink.getListSizes(), std::vector<size_t> {personCount});
    EXPECT_EQ(sink.getCounts(), std::vector<uint64_t> {personCount});
}

TEST_F(ReducedRowOutputTest, rejectsATallyReadByAStepThatKeepsManyRows) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    DiscardingSink sink;
    EXPECT_THROW(runProgram(scanLoopReadsTheTally, reader.getView(), sink), IRException);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
