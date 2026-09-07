#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "IRTestRows.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// A join on a boolean property: the four French people of SimpleGraph key alike, the four
// who are not key alike, and the ten nodes with no isFrench at all key null and match
// nothing.
const char* const joinOnIsFrench = R"mlir(
func.func @main() {
  %0:4 = db.hash_join factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } on 1, 1
  db.output(%0#0, %0#2) names ["n", "m"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A join on a string property every node carries and no two share, so the result is the
// diagonal: one row per node, pairing it with itself.
const char* const joinOnName = R"mlir(
func.func @main() {
  %0:4 = db.hash_join factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } on 1, 1
  db.output(%0#0, %0#2) names ["n", "m"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

using Blocks = std::vector<std::vector<size_t>>;

Row pair(size_t left, size_t right) {
    return Row {std::to_string(left), std::to_string(right)};
}

}

// db.hash_join run at chunk sizes that split the build side and the probe side several
// ways. The build rows are numbered from where the previous chunk ended and the index is
// filled before any probing, so the rows the join emits - and the order it emits them in -
// must not depend on how the scans were chunked. That is what these cases pin.
class HashJoinChunkedTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    // Parses a db-dialect program, lowers it to nl and runs the lowered function against
    // the graph at this chunk size.
    void runProgram(const char* programText, size_t chunkSize, RowSink& sink) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

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

    void expectRowsAtEveryChunkSize(const char* programText, const Rows& expected) {
        for (const size_t chunkSize : _chunkSizes) {
            RowSink sink;
            runProgram(programText, chunkSize, sink);

            EXPECT_EQ(sink.rows(), expected) << "at chunk size " << chunkSize;
        }
    }

    // One row per chunk, then two, three and five - each splitting the eighteen nodes
    // differently - and last the production size, which holds them all in one chunk.
    const std::vector<size_t> _chunkSizes {1, 2, 3, 5, ChunkConfig::CHUNK_SIZE};

    std::unique_ptr<Graph> _graph;
};

// Remy (0), Adam (1), Maxime (8) and Luc (9) are French; Martina (11), Suhas (12),
// Cyrus (15) and Doruk (17) are not. The join is the two blocks, and the interests - which
// carry no isFrench - match nothing, not even each other.
TEST_F(HashJoinChunkedTest, joinsTheTwoBlocksOfABooleanKey) {
    const Blocks blocks {{0, 1, 8, 9}, {11, 12, 15, 17}};

    // The probe walks its rows in scan order and each row's matches come out in build
    // order, so the rows are the blocks read one probe node at a time.
    Rows expected;
    for (size_t probeNode = 0; probeNode < 18; probeNode++) {
        for (const std::vector<size_t>& block : blocks) {
            if (std::find(block.begin(), block.end(), probeNode) == block.end()) {
                continue;
            }

            for (const size_t buildNode : block) {
                expected.push_back(pair(probeNode, buildNode));
            }
        }
    }

    expectRowsAtEveryChunkSize(joinOnIsFrench, expected);
}

TEST_F(HashJoinChunkedTest, joinsEveryNodeWithItselfOnAStringKey) {
    Rows expected;
    for (size_t node = 0; node < 18; node++) {
        expected.push_back(pair(node, node));
    }

    expectRowsAtEveryChunkSize(joinOnName, expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
