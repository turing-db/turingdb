#include <gtest/gtest.h>

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
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;
using namespace turing::test;

namespace {

// MATCH (n:Person)((a)-[e]->(b:Person)){1,3}(m) RETURN n, m, size(e) as codegen leaves it: the
// label a hop's end must carry is a predicate region over the hop
const char* const regionProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

const char* const fusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 hop_labels ["Person"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// MATCH (n)((a)-[e]-(b:Person)){2,4}(m) RETURN DISTINCT n, m in both forms: the distinct walk
// over an undirected hop
const char* const distinctRegionProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 2 to 4 distinct {
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

const char* const distinctFusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 2 to 4 hop_labels ["Person"] distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A label no node carries: no hop passes, and only the zero-length paths are left
const char* const unknownLabelRegionProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 0 to 2 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["NoSuchLabel"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

const char* const unknownLabelFusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 0 to 2 hop_labels ["NoSuchLabel"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// The label is asked of the hop's source, which hop_labels does not say
const char* const sourceLabelRegionProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%src) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const emptyHopLabelsProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 hop_labels [] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const blankHopLabelProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 hop_labels [""] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const hopLabelsAndRegionProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 hop_labels ["Person"] {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%src) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

class ExploreHopLabelsTest : public ::testing::Test {
protected:
    ExploreHopLabelsTest() {
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

    static void expectHopLabels(mlir::db::ExplorePaths exploration, const std::vector<std::string>& expected) {
        const std::optional<mlir::ArrayAttr> hopLabels = exploration.getHopLabels();
        ASSERT_TRUE(hopLabels.has_value());

        std::vector<std::string> labels;
        for (const mlir::Attribute label : *hopLabels) {
            labels.push_back(mlir::cast<mlir::StringAttr>(label).getValue().str());
        }
        EXPECT_EQ(labels, expected);
    }

    void runPass(mlir::ModuleOp module, std::unique_ptr<mlir::Pass> pass) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(std::move(pass));
        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
    }

    void runModule(mlir::ModuleOp dbModule, const GraphView& view, RowSink& sink, size_t chunkSize) {
        const mlir::func::FuncOp dbFunction = dbModule.lookupSymbol<mlir::func::FuncOp>("main");
        const mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));

        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    void runProgram(const char* programText, const GraphView& view, RowSink& sink, size_t chunkSize) {
        const mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(programText);
        ASSERT_TRUE(dbModule);

        runModule(*dbModule, view, sink, chunkSize);
    }

    // The fused form must emit the rows the region form does, chunk size by chunk size
    void expectSameRows(const char* regionProgramText, const char* fusedProgramText, const GraphView& view) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink region;
            runProgram(regionProgramText, view, region, chunkSize);

            RowSink fused;
            runProgram(fusedProgramText, view, fused, chunkSize);

            Rows expected;
            region.sortedRows(expected);

            Rows actual;
            fused.sortedRows(actual);

            EXPECT_FALSE(expected.empty()) << "chunk size " << chunkSize;
            EXPECT_EQ(actual, expected) << "chunk size " << chunkSize;
        }
    }

    mlir::MLIRContext _context;
};

class ExploreHopLabelsSimpleGraphTest : public ExploreHopLabelsTest {
protected:
    ExploreHopLabelsSimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

}

TEST_F(ExploreHopLabelsTest, fusesALabelOnlyHopRegionIntoHopLabels) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(regionProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseExploreHopLabels());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    expectHopLabels(exploration, {"Person"});
    EXPECT_TRUE(exploration.getHop().empty());
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);
}

TEST_F(ExploreHopLabelsTest, leavesARegionAskingTheHopSourceForLabels) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(sourceLabelRegionProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseExploreHopLabels());

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    EXPECT_FALSE(exploration.getHopLabels().has_value());
    EXPECT_FALSE(exploration.getHop().empty());
}

TEST_F(ExploreHopLabelsTest, roundTripsTheHopLabelsThroughThePrinter) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(fusedProgram);
    ASSERT_TRUE(module);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);
    EXPECT_NE(printed.find("hop_labels [\"Person\"]"), std::string::npos) << printed;

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed) << printed;
    expectHopLabels(findExplorePaths(*reparsed), {"Person"});
}

TEST_F(ExploreHopLabelsTest, rejectsMalformedHopLabels) {
    for (const char* program : {emptyHopLabelsProgram, blankHopLabelProgram, hopLabelsAndRegionProgram}) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        EXPECT_FALSE(module) << program;
    }
}

TEST_F(ExploreHopLabelsSimpleGraphTest, fusedFormsEmitTheRegionRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    expectSameRows(regionProgram, fusedProgram, view);
    expectSameRows(distinctRegionProgram, distinctFusedProgram, view);
    expectSameRows(unknownLabelRegionProgram, unknownLabelFusedProgram, view);
}
