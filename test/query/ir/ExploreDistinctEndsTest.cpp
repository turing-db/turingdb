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

// MATCH (n)-[e]->{1,3}(m) RETURN DISTINCT n, m: the dedup reads the seed and the end alone
const char* const dedupProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A property filter on the end between the exploration and the dedup: still one set
const char* const filteredDedupProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %age = db.get_node_properties(%0#1, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %c = db.constant(30 : i64)
  %mask = db.gt %age, %c : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %1:2 = db.filter(%mask, {%0#0, %0#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %d:2 = db.remove_duplicates(%1#0, %1#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]->{1,3}(m) RETURN count(DISTINCT m)
const char* const countDistinctProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %c = db.count(%0#1) distinct : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%c) : !db.column<ui64>
  return
}
)mlir";

// MATCH (n)-[e]->{1,3}(m)-->(p) RETURN DISTINCT n, p: a plain hop before the dedup
const char* const hopThenDedupProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %h:5 = db.get_out_edges(%0#1, {%0#0}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %d:2 = db.remove_duplicates(%h#4, %h#3) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]->{1,3}(m) RETURN count(m): a tally of rows, not a set
const char* const plainCountProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %c = db.count(%0#1) : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%c) : !db.column<ui64>
  return
}
)mlir";

// MATCH (n)-[e]->{1,3}(m) RETURN n, m: every row goes out
const char* const outputProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A LIMIT before the dedup counts rows
const char* const limitThenDedupProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l:2 = db.limit(%0#0, %0#1) count 3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %d:2 = db.remove_duplicates(%l#0, %l#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]->{1,3}(m) RETURN DISTINCT n, e, m: the path is read
const char* const pathDedupProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  %d:3 = db.remove_duplicates(%0#0, %e, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1, %d#2) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]->{2,3}(m) RETURN DISTINCT n, m: a walk of two hops may have no trail
const char* const minTwoDedupProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 2 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]-{1,3}(m) RETURN DISTINCT n, m: undirected, the one-edge backtrack is a closed
// walk of two hops with no trail behind it
const char* const undirectedMinOneDedupProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-[e]-{0,3}(m:Person) RETURN DISTINCT n, m in both forms: an end label
// and both directions
const char* const bothEnumeratedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 0 to 3 end_labels ["Person"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const bothDistinctProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 0 to 3 end_labels ["Person"] distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)((a)-[e]->(b:Person))*(m) RETURN DISTINCT n, m in both forms: a hop predicate
const char* const hopEnumeratedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 0 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const hopDistinctProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 0 distinct {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]->{1,3}(m) RETURN DISTINCT n, m over the generated graph, in both forms
const char* const generatedEnumeratedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const generatedDistinctProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

class ExploreDistinctEndsTest : public ::testing::Test {
protected:
    ExploreDistinctEndsTest() {
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

    void runPass(mlir::ModuleOp module, std::unique_ptr<mlir::Pass> pass) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(std::move(pass));
        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
    }

    // Whether the pass marks the program's exploration distinct
    bool marksDistinct(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        if (!module) {
            ADD_FAILURE() << programText;
            return false;
        }

        runPass(*module, mlir::db::createFuseExploreDistinctEnds());
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module))) << programText;

        return findExplorePaths(*module).getDistinct();
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

    // The distinct form must emit the rows the enumerated form does after its dedup
    void expectSameRows(const char* enumeratedProgram, const char* distinctProgram, const GraphView& view) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink enumerated;
            runProgram(enumeratedProgram, view, enumerated, chunkSize);

            RowSink distinct;
            runProgram(distinctProgram, view, distinct, chunkSize);

            Rows expected;
            enumerated.sortedRows(expected);

            Rows actual;
            distinct.sortedRows(actual);

            EXPECT_FALSE(expected.empty()) << "chunk size " << chunkSize;
            EXPECT_EQ(actual, expected) << "chunk size " << chunkSize;
        }
    }

    mlir::MLIRContext _context;
};

class ExploreDistinctEndsSimpleGraphTest : public ExploreDistinctEndsTest {
protected:
    ExploreDistinctEndsSimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

class ExploreDistinctEndsGeneratedGraphTest : public ExploreDistinctEndsTest {
protected:
    static constexpr size_t nodeCount = 600;
    static constexpr size_t outDegree = 3;

    ExploreDistinctEndsGeneratedGraphTest()
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

        uint64_t state = 4242;
        for (const NodeID source : nodes) {
            for (size_t edge = 0; edge < outDegree; edge++) {
                state = state * 6364136223846793005ull + 1442695040888963407ull;
                builder.addEdge(type, source, nodes[(state >> 33) % nodeCount]);
            }
        }

        const auto submitted = change->access().submit(_jobSystem);
        EXPECT_TRUE(submitted);
    }

    ~ExploreDistinctEndsGeneratedGraphTest() override {
        _jobSystem.terminate();
    }

    JobSystem _jobSystem;
    std::unique_ptr<Graph> _graph;
};

}

TEST_F(ExploreDistinctEndsTest, marksExplorationsOnlyEverReadAsASet) {
    EXPECT_TRUE(marksDistinct(dedupProgram));
    EXPECT_TRUE(marksDistinct(filteredDedupProgram));
    EXPECT_TRUE(marksDistinct(countDistinctProgram));
    EXPECT_TRUE(marksDistinct(hopThenDedupProgram));
    EXPECT_TRUE(marksDistinct(bothEnumeratedProgram));
    EXPECT_TRUE(marksDistinct(hopEnumeratedProgram));
}

TEST_F(ExploreDistinctEndsTest, leavesExplorationsWhoseRowsAreCounted) {
    EXPECT_FALSE(marksDistinct(plainCountProgram));
    EXPECT_FALSE(marksDistinct(outputProgram));
    EXPECT_FALSE(marksDistinct(limitThenDedupProgram));
    EXPECT_FALSE(marksDistinct(pathDedupProgram));
    EXPECT_FALSE(marksDistinct(minTwoDedupProgram));
    EXPECT_FALSE(marksDistinct(undirectedMinOneDedupProgram));
}

TEST_F(ExploreDistinctEndsSimpleGraphTest, distinctFormsEmitTheDeduplicatedRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    expectSameRows(bothEnumeratedProgram, bothDistinctProgram, view);
    expectSameRows(hopEnumeratedProgram, hopDistinctProgram, view);
}

TEST_F(ExploreDistinctEndsSimpleGraphTest, passedProgramsEmitTheDeduplicatedRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    for (const char* program : {dedupProgram, filteredDedupProgram, countDistinctProgram, hopThenDedupProgram}) {
        RowSink enumerated;
        runProgram(program, view, enumerated);

        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module);
        runPass(*module, mlir::db::createFuseExploreDistinctEnds());
        ASSERT_TRUE(findExplorePaths(*module).getDistinct());

        RowSink distinct;
        runModule(*module, view, distinct);

        Rows expected;
        enumerated.sortedRows(expected);

        Rows actual;
        distinct.sortedRows(actual);

        EXPECT_FALSE(expected.empty()) << program;
        EXPECT_EQ(actual, expected) << program;
    }
}

TEST_F(ExploreDistinctEndsGeneratedGraphTest, distinctFormEmitsTheDeduplicatedRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ASSERT_EQ(reader.getNodeCount(), nodeCount);

    expectSameRows(generatedEnumeratedProgram, generatedDistinctProgram, reader.getView());
}
