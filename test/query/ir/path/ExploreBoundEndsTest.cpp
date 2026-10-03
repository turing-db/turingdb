#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "IRTestRows.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOps.h"
#include "StorageDialect.h"
#include "StorageTypes.h"

#include "Graph.h"
#include "LocalMemory.h"
#include "SimpleGraph.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathTargetIndex.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

// MATCH (a:Person)-[e]->+(b), (a)-->(b) RETURN a, e, b as codegen leaves it: the exploration
// carries the bound b and an equality filter keeps the paths ending on it
const char* const boundFilterProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %m = db.eq %0#1, %0#3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %1:4 = db.filter(%m, {%0#0, %0#1, %0#2, %0#3}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %e = db.expand_path(%1#2, %1#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%1#0, %e, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same, the equality written the other way round
const char* const swappedEqualityProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %m = db.eq %0#3, %0#1 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %1:4 = db.filter(%m, {%0#0, %0#1, %0#2, %0#3}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%1#0, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same query once the equality is folded into the exploration
const char* const boundFusedProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[e]-{0,3}(b), (a)-->(b) RETURN a, e, b from every node, in both forms
const char* const bothBoundFilterProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) both hops 0 to 3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %m = db.eq %0#1, %0#3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %1:4 = db.filter(%m, {%0#0, %0#1, %0#2, %0#3}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %e = db.expand_path(%1#2, %1#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%1#0, %e, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const bothBoundFusedProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) both hops 0 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// The equality compares the end with the seed, which no carried column holds: the walk the
// pattern brings back to where it started
const char* const seedEqualityProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %m = db.eq %0#1, %0#0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%m, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%1#0, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The equality reads a column the exploration did not carry
const char* const outsideEqualityProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:3 = db.explore_paths(%h#0, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %m = db.eq %0#1, %h#3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%m, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%1#0, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Two carried columns, the end among them second and the first unread afterwards
const char* const trimProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:5 = db.explore_paths(%h#0, {%h#1, %h#3}) forward hops 1 end_column 1 : (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const distinctProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const endColumnOutOfRangeProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 end_column 1 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const endColumnNotANodeProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#1}) forward hops 1 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.edge_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.edge_id>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const distinctMinTwoProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 2 distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const distinctUndirectedMinOneProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 1 distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const distinctReadsPathProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%e) : !db.column<!storage.list<!storage.edge_id>>
  return
}
)mlir";

// MATCH (n)-->(b), (n)-[e]->{1,3}(b) RETURN n, b, size(e) over the generated graph: enough
// seeds and targets for the executor to build the target index
const char* const generatedFilterProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 to 3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %m = db.eq %0#1, %0#3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %1:4 = db.filter(%m, {%0#0, %0#1, %0#2, %0#3}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %l = db.path_length(%1#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%1#0, %1#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

const char* const generatedFusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// MATCH (n)-->(b), (n)-[e]->{2,3}(b) RETURN DISTINCT n, b over the generated graph: past a
// minimum of one hop the distinct form walks, and takes the target index
const char* const generatedWalkedFilterProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 2 to 3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %m = db.eq %0#1, %0#3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %1:4 = db.filter(%m, {%0#0, %0#1, %0#2, %0#3}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %d:2 = db.remove_duplicates(%1#0, %1#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const generatedWalkedDistinctProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%n, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 2 to 3 end_column 0 distinct : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

class ExploreBoundEndsTest : public ::testing::Test {
protected:
    ExploreBoundEndsTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    static mlir::db::ExplorePaths findExplorePaths(mlir::ModuleOp module) {
        mlir::db::ExplorePaths found;
        module.walk([&](mlir::db::ExplorePaths op) {
            found = op;
        });

        return found;
    }

    template <typename Op>
    static size_t countOps(mlir::ModuleOp module) {
        size_t count = 0;
        module.walk([&](Op) {
            count++;
        });

        return count;
    }

    void runPass(mlir::ModuleOp module, std::unique_ptr<mlir::Pass> pass) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(std::move(pass));
        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
    }

    mlir::OwningOpRef<mlir::ModuleOp> lower(mlir::ModuleOp dbModule, const GraphView& view) {
        const mlir::func::FuncOp dbFunction = dbModule.lookupSymbol<mlir::func::FuncOp>("main");
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));

        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        return nlModule;
    }

    void runModule(mlir::ModuleOp dbModule, const GraphView& view, RowSink& sink, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        const mlir::OwningOpRef<mlir::ModuleOp> nlModule = lower(dbModule, view);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    void runProgram(const char* programText, const GraphView& view, RowSink& sink, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        const mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(programText);
        ASSERT_TRUE(dbModule);

        runModule(*dbModule, view, sink, chunkSize);
    }

    void expectSameRows(const char* filterProgram, const char* fusedProgram, const GraphView& view) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink filtered;
            runProgram(filterProgram, view, filtered, chunkSize);

            RowSink fused;
            runProgram(fusedProgram, view, fused, chunkSize);

            Rows expected;
            filtered.sortedRows(expected);

            Rows actual;
            fused.sortedRows(actual);

            EXPECT_FALSE(expected.empty()) << "chunk size " << chunkSize;
            EXPECT_EQ(actual, expected) << "chunk size " << chunkSize;
        }
    }

    mlir::MLIRContext _context;
};

class ExploreBoundEndsSimpleGraphTest : public ExploreBoundEndsTest {
protected:
    ExploreBoundEndsSimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

// A generated graph with five pseudo-random out-edges per node: bound to their out-neighbours,
// its nodes give the executor's cost gate enough seeds per batch of targets
class ExploreBoundEndsGeneratedGraphTest : public ExploreBoundEndsTest {
protected:
    static constexpr size_t nodeCount = 600;
    static constexpr size_t outDegree = 5;

    ExploreBoundEndsGeneratedGraphTest()
        : _graph(Graph::create())
    {
        _jobSystem.init();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const EdgeTypeID type = metadata.getOrCreateEdgeType("A");

        std::vector<NodeID> nodes;
        for (size_t node = 0; node < nodeCount; node++) {
            nodes.push_back(builder.addNode(plain));
        }

        uint64_t state = 98765;
        for (const NodeID source : nodes) {
            for (size_t edge = 0; edge < outDegree; edge++) {
                state = state * 6364136223846793005ull + 1442695040888963407ull;
                builder.addEdge(type, source, nodes[(state >> 33) % nodeCount]);
            }
        }

        const auto submitted = change->access().submit(_jobSystem);
        EXPECT_TRUE(submitted);
    }

    ~ExploreBoundEndsGeneratedGraphTest() override {
        _jobSystem.terminate();
    }

    JobSystem _jobSystem;
    std::unique_ptr<Graph> _graph;
};

}

TEST_F(ExploreBoundEndsTest, roundTripsTheEndColumnAndTheDistinctFlag) {
    for (const char* program : {boundFusedProgram, distinctProgram}) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module) << program;
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module->print(stream);

        const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
        ASSERT_TRUE(reparsed) << printed;

        mlir::db::ExplorePaths original = findExplorePaths(*module);
        mlir::db::ExplorePaths copy = findExplorePaths(*reparsed);
        EXPECT_EQ(original.getEndColumn(), copy.getEndColumn());
        EXPECT_EQ(original.getDistinct(), copy.getDistinct());
    }

    EXPECT_EQ(findExplorePaths(*parse(boundFusedProgram)).getEndColumn(), std::optional<uint64_t> {0});
    EXPECT_FALSE(findExplorePaths(*parse(boundFusedProgram)).getDistinct());
    EXPECT_TRUE(findExplorePaths(*parse(distinctProgram)).getDistinct());
    EXPECT_FALSE(findExplorePaths(*parse(distinctProgram)).getEndColumn().has_value());
}

TEST_F(ExploreBoundEndsTest, rejectsMalformedBoundsAndDistinctFlags) {
    for (const char* program : {endColumnOutOfRangeProgram, endColumnNotANodeProgram, distinctReadsPathProgram}) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        EXPECT_FALSE(module) << program;
    }
}

TEST_F(ExploreBoundEndsTest, acceptsDistinctAtAnyBoundAndDirection) {
    for (const char* program : {distinctMinTwoProgram, distinctUndirectedMinOneProgram}) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        EXPECT_TRUE(module) << program;
    }
}

TEST_F(ExploreBoundEndsTest, fusesTheEqualityFilterIntoTheExploration) {
    for (const char* program : {boundFilterProgram, swappedEqualityProgram}) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module) << program;

        runPass(*module, mlir::db::createFuseExploreEndNodes());
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

        mlir::db::ExplorePaths fused = findExplorePaths(*module);
        ASSERT_TRUE(fused);
        EXPECT_EQ(fused.getEndColumn(), std::optional<uint64_t> {0}) << program;
        EXPECT_EQ(fused.getColumnsToFilter().size(), 1u);
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u) << program;
        EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u) << program;
    }
}

TEST_F(ExploreBoundEndsTest, fusesTheEqualityAgainstTheSeedIntoTheExploration) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(seedEqualityProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseExploreEndNodes());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    EXPECT_TRUE(fused.getEndsOnSeed());
    EXPECT_FALSE(fused.getEndColumn().has_value());
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u);
}

TEST_F(ExploreBoundEndsTest, leavesEqualitiesItCannotTake) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(outsideEqualityProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseExploreEndNodes());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    EXPECT_FALSE(findExplorePaths(*module).getEndColumn().has_value());
    EXPECT_FALSE(findExplorePaths(*module).getEndsOnSeed());
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
}

TEST_F(ExploreBoundEndsTest, trimKeepsAndRenumbersTheEndColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(trimProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createTrimUnreadColumns());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    // The unread edge carry goes, the end carry stays though nothing reads its result
    mlir::db::ExplorePaths trimmed = findExplorePaths(*module);
    ASSERT_TRUE(trimmed);
    EXPECT_EQ(trimmed.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(trimmed.getEndColumn(), std::optional<uint64_t> {0});
    EXPECT_TRUE(mlir::isa<mlir::storage::NodeIDType>(
        mlir::cast<mlir::db::ColumnType>(trimmed.getColumnsToFilter().front().getType()).getType()));
}

TEST_F(ExploreBoundEndsSimpleGraphTest, lowersTheEndColumnAndTheDistinctFlag) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const mlir::OwningOpRef<mlir::ModuleOp> bound = parse(boundFusedProgram);
    ASSERT_TRUE(bound);
    mlir::OwningOpRef<mlir::ModuleOp> boundLowered = lower(*bound, reader.getView());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*boundLowered)));

    mlir::nl::ExplorePaths boundExploration;
    boundLowered->walk([&](mlir::nl::ExplorePaths found) {
        boundExploration = found;
    });
    ASSERT_TRUE(boundExploration);
    EXPECT_EQ(boundExploration.getEndColumn(), std::optional<uint64_t> {0});
    EXPECT_FALSE(boundExploration.getDistinct());

    const mlir::OwningOpRef<mlir::ModuleOp> distinct = parse(distinctProgram);
    ASSERT_TRUE(distinct);
    mlir::OwningOpRef<mlir::ModuleOp> distinctLowered = lower(*distinct, reader.getView());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*distinctLowered)));

    mlir::nl::ExplorePaths distinctExploration;
    distinctLowered->walk([&](mlir::nl::ExplorePaths found) {
        distinctExploration = found;
    });
    ASSERT_TRUE(distinctExploration);
    EXPECT_TRUE(distinctExploration.getDistinct());
}

TEST_F(ExploreBoundEndsSimpleGraphTest, fusedFormsEmitTheFilteredRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    expectSameRows(boundFilterProgram, boundFusedProgram, view);
    expectSameRows(bothBoundFilterProgram, bothBoundFusedProgram, view);
}

TEST_F(ExploreBoundEndsSimpleGraphTest, boundEndsKeepOnlyThePathsLandingOnTheDirectNeighbour) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    RowSink sink;
    runProgram(boundFusedProgram, reader.getView(), sink);

    Rows rows;
    sink.sortedRows(rows);

    // Every row's end is the neighbour the plain hop bound, so the seed and end of each row
    // are joined by a direct edge; Remy (0) reaches Adam (1) directly and around the cycle
    for (const Row& row : rows) {
        ASSERT_EQ(row.size(), 3u);
        EXPECT_NE(row[0], row[2]);
    }
    EXPECT_NE(std::find(rows.begin(), rows.end(), Row {"0", "[0]", "1"}), rows.end());
    EXPECT_NE(std::find(rows.begin(), rows.end(), Row {"0", "[1, 7, 0]", "1"}), rows.end());
    EXPECT_EQ(std::find(rows.begin(), rows.end(), Row {"0", "[0, 4]", "0"}), rows.end());
}

TEST_F(ExploreBoundEndsGeneratedGraphTest, targetIndexKeepsTheFilteredRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    ASSERT_EQ(reader.getNodeCount(), nodeCount);

    // Every node's out-neighbours are the targets: at most one batch per sixty-four of
    // them, and the seeds are the edges
    std::vector<NodeID> seeds;
    for (size_t node = 0; node < nodeCount; node++) {
        seeds.push_back(NodeID(node));
    }

    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(PartDirectory(view), PathExplorationDir::FORWARD, {}, seeds, expansion);
    EXPECT_TRUE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, {}, expansion, nodeCount * outDegree, nodeCount, 3));

    RowSink filtered;
    runProgram(generatedFilterProgram, view, filtered);

    RowSink fused;
    runProgram(generatedFusedProgram, view, fused);

    Rows expected;
    filtered.sortedRows(expected);

    Rows actual;
    fused.sortedRows(actual);

    EXPECT_FALSE(expected.empty());
    EXPECT_EQ(actual, expected);
}

TEST_F(ExploreBoundEndsGeneratedGraphTest, distinctWalkWithTheTargetIndexKeepsTheDeduplicatedRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    ASSERT_EQ(reader.getNodeCount(), nodeCount);

    std::vector<NodeID> seeds;
    for (size_t node = 0; node < nodeCount; node++) {
        seeds.push_back(NodeID(node));
    }

    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(PartDirectory(view), PathExplorationDir::FORWARD, {}, seeds, expansion);
    EXPECT_TRUE(PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, {}, expansion, nodeCount * outDegree, nodeCount, 3));

    RowSink filtered;
    runProgram(generatedWalkedFilterProgram, view, filtered);

    RowSink distinct;
    runProgram(generatedWalkedDistinctProgram, view, distinct);

    Rows expected;
    filtered.sortedRows(expected);

    Rows actual;
    distinct.sortedRows(actual);

    EXPECT_FALSE(expected.empty());
    EXPECT_EQ(actual, expected);
}
