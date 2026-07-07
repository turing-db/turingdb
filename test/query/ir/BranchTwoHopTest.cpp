#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
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

// Accumulates every emitted node ID across all output chunks into one vector.
// The branch program outputs a single node-ID column - the returned `a`.
class CollectingNodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(nodeIDs, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _nodeIDs.push_back((*nodeIDs)[rowIndex].getValue());
        }
    }

    const std::vector<uint64_t>& getNodeIDs() const { return _nodeIDs; }

private:
    std::vector<uint64_t> _nodeIDs;
};

// MATCH (a)->(b)->(c), (b)->(d) RETURN a. The branch off `b` reconnects to `b`
// (%srcs2, the second hop's source), not to the chain tip `c`, and carries `a`
// (and `c`) through, so the emitted %aBranch is filtered/replicated to one row
// per full (a, b, c, d) match. Returning the second hop's `a` (%aCarried)
// instead would drop the branch's x-d multiplicity and undercount `a`. This is
// the lowered nl form of samples/mlir/branch.mlir (see samples/mlir/branch.nl.mlir).
constexpr const char* branchTwoHopProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %hop = nl.get_out_edges(%b, {%srcs}) : !nl.chunk<!storage.node_id>
      nl.for %srcs2, %eids2, %etypes2, %c, %aCarried in %hop : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
        %branch = nl.get_out_edges(%srcs2, {%aCarried, %c}) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
        nl.for %srcs3, %eids3, %etypes3, %d, %aBranch, %cBranch in %branch : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
          nl.output(%aBranch) : !nl.chunk<!storage.node_id>
        }
      }
    }
  }
  func.return
}
)mlir";

}

class BranchTwoHopTest : public TuringTest {
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

// MATCH (a)->(b)->(c), (b)->(d) RETURN a on the simpledb graph - the query and
// graph on which the wrong `a` count was reported. Only three nodes are a valid
// `b` (one with a predecessor `a` and at least one successor); `c` and `d` each
// range over all of `b`'s successors independently:
//   b = Remy (0):   a in {Adam (1), Ghosts (6)}, out-degree 4 -> 2 * 4 * 4 = 32
//   b = Adam (1):   a = Remy (0),                 out-degree 3 -> 1 * 3 * 3 = 9
//   b = Ghosts (6): a = Remy (0),                 out-degree 1 -> 1 * 1 * 1 = 1
// so 42 rows of `a`: Remy x10 (9 via Adam + 1 via Ghosts), Adam x16, Ghosts x16.
// Returning the second hop's `a` instead of the branch-filtered one drops the
// x-d factor and undercounts to 12 - the bug this test guards against.
TEST_F(BranchTwoHopTest, returnsBranchCarriedSourceCount) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(branchTwoHopProgram, reader.getView(), sink);

    std::vector<uint64_t> nodeIDs = sink.getNodeIDs();
    std::sort(nodeIDs.begin(), nodeIDs.end());

    // Remy (0) x10, Adam (1) x16, Ghosts (6) x16, already in sorted order.
    std::vector<uint64_t> expected;
    expected.insert(expected.end(), 10, 0);
    expected.insert(expected.end(), 16, 1);
    expected.insert(expected.end(), 16, 6);

    EXPECT_EQ(nodeIDs.size(), 42u);
    EXPECT_EQ(nodeIDs, expected);
}
