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
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;
using namespace turing::test;

namespace {

// MATCH (n)-[e:KNOWS]->{1,3}(m) RETURN n, e, m, size(e), no carry set
const char* const boundedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 edge_type "KNOWS" : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %e, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// MATCH (a)-->(b)((x)-[e]-(y) WHERE y:Person)*(m) RETURN b, m carrying a: an unbounded
// exploration in both directions from b, with a hop predicate and one carried column
const char* const hopRegionProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#3, {%h#0}) both hops 0 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%0#0, %0#1, %0#3) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const maxBelowMinProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 3 to 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const emptyEdgeTypeProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 edge_type "" : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const carryMismatchProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {%n}) forward hops 1 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const hopWrongArgumentsProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 {
  ^bb0(%src: !db.column<!storage.node_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const hopYieldsNodesProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    db.yield %end : !db.column<!storage.node_id>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const hopReadsOutsideProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %outside = db.get_node_label_set(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ok = db.check_label_constraint(%outside, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const expandWrongListProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.node_id>>
  db.output(%e) : !db.column<!storage.list<!storage.node_id>>
  return
}
)mlir";

// Two columns carried through the exploration, only the second read afterwards
const char* const trimProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:5 = db.explore_paths(%h#3, {%h#0, %h#1}) forward hops 1 to 2 edge_type "KNOWS_WELL" {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>)
  db.output(%0#1, %0#4) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>
  return
}
)mlir";

// A predicate on the seed's property and one on the end's property, both after the
// exploration: the first can move above it, the second cannot
const char* const pushDownProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %age = db.get_node_properties(%0#0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %c = db.constant(30 : i64)
  %seedMask = db.gt %age, %c : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%seedMask, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %endAge = db.get_node_properties(%1#1, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %endMask = db.gt %endAge, %c : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %2:3 = db.filter(%endMask, {%1#0, %1#1, %1#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%2#0, %2#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-[e:NOPE]->*(m) RETURN n, e, m: no edge of that type exists, so every
// Person is its own zero-length path and nothing else
const char* const unmatchableTypeProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 0 edge_type "NOPE" : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-[e]->+(m:Person) RETURN n, e, m, the first variable-length oracle,
// with the end label kept as a filter after the exploration
const char* const personToPersonProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%1#2, %1#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  %l = db.path_length(%1#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%1#0, %e, %1#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// The same paths cut per hop instead: every hop must end on a Person, so a path through
// Ghosts is never walked, where the filter form above keeps Remy->Ghosts->Remy
const char* const personHopsProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 4 {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %ls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %ok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-[e]->{2,2}(m) RETURN n, sources, ends, m: the two node lists of a
// two-hop path, read through the same trie
const char* const groupVariablesProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 2 to 2 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %s = db.expand_path(%0#2, %0#0) kind sources : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.node_id>>
  %t = db.expand_path(%0#2, %0#0) kind ends : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.node_id>>
  db.output(%0#0, %s, %t, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.node_id>>, !db.column<!storage.list<!storage.node_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// A path column output straight from a small chunk: the expansion at output is per step
const char* const rawPathOutputProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 2 edge_type "KNOWS_WELL" : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#2, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>
  return
}
)mlir";

class ExplorePathsDialectTest : public ::testing::Test {
protected:
    ExplorePathsDialectTest() {
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

    mlir::OwningOpRef<mlir::ModuleOp> lower(mlir::ModuleOp dbModule, const GraphView& view) {
        const mlir::func::FuncOp dbFunction = dbModule.lookupSymbol<mlir::func::FuncOp>("main");
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));

        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        return nlModule;
    }

    void runProgram(const char* programText, const GraphView& view, RowSink& sink, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        const mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(programText);
        ASSERT_TRUE(dbModule);

        const mlir::OwningOpRef<mlir::ModuleOp> nlModule = lower(*dbModule, view);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    mlir::MLIRContext _context;
};

class ExplorePathsSimpleGraphTest : public ExplorePathsDialectTest {
protected:
    ExplorePathsSimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

}

TEST_F(ExplorePathsDialectTest, roundTripsThroughThePrinter) {
    for (const char* program : {boundedProgram, hopRegionProgram}) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module);
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module->print(stream);

        const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
        ASSERT_TRUE(reparsed) << printed;
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*reparsed)));

        mlir::db::ExplorePaths original = findExplorePaths(*module);
        mlir::db::ExplorePaths copy = findExplorePaths(*reparsed);
        ASSERT_TRUE(original);
        ASSERT_TRUE(copy);
        EXPECT_EQ(original.getDirection(), copy.getDirection());
        EXPECT_EQ(original.getMinHops(), copy.getMinHops());
        EXPECT_EQ(original.getMaxHops(), copy.getMaxHops());
        EXPECT_EQ(original.getEdgeType(), copy.getEdgeType());
        EXPECT_EQ(original.getHop().empty(), copy.getHop().empty());
    }
}

TEST_F(ExplorePathsDialectTest, readsTheAttributesAndTheRegion) {
    const mlir::OwningOpRef<mlir::ModuleOp> bounded = parse(boundedProgram);
    ASSERT_TRUE(bounded);

    mlir::db::ExplorePaths boundedOp = findExplorePaths(*bounded);
    EXPECT_EQ(boundedOp.getDirection(), mlir::storage::PathDirection::Forward);
    EXPECT_EQ(boundedOp.getMinHops(), 1u);
    EXPECT_EQ(boundedOp.getMaxHops(), std::optional<uint64_t> {3});
    EXPECT_EQ(boundedOp.getEdgeType(), std::optional<llvm::StringRef> {"KNOWS"});
    EXPECT_TRUE(boundedOp.getHop().empty());
    EXPECT_EQ(boundedOp.getColumnsToFilter().size(), 0u);

    const mlir::OwningOpRef<mlir::ModuleOp> region = parse(hopRegionProgram);
    ASSERT_TRUE(region);

    mlir::db::ExplorePaths regionOp = findExplorePaths(*region);
    EXPECT_EQ(regionOp.getDirection(), mlir::storage::PathDirection::Both);
    EXPECT_EQ(regionOp.getMinHops(), 0u);
    EXPECT_FALSE(regionOp.getMaxHops().has_value());
    EXPECT_FALSE(regionOp.getEdgeType().has_value());
    ASSERT_FALSE(regionOp.getHop().empty());
    EXPECT_EQ(regionOp.getHop().front().getNumArguments(), 3u);
    EXPECT_EQ(regionOp.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(regionOp.getFilteredColumns().size(), 1u);
}

TEST_F(ExplorePathsDialectTest, rejectsMalformedExplorations) {
    for (const char* program : {maxBelowMinProgram,
                                emptyEdgeTypeProgram,
                                carryMismatchProgram,
                                hopWrongArgumentsProgram,
                                hopYieldsNodesProgram,
                                hopReadsOutsideProgram,
                                expandWrongListProgram}) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        EXPECT_FALSE(module) << program;
    }
}

TEST_F(ExplorePathsDialectTest, trimDropsUnreadCarriesAndKeepsTheRest) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(trimProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createTrimUnreadColumns());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ExplorePaths trimmed = findExplorePaths(*module);
    ASSERT_TRUE(trimmed);

    // Only the edge column is read after the exploration, so the node carry is gone;
    // the attributes and the hop region survive the rebuild
    EXPECT_EQ(trimmed.getColumnsToFilter().size(), 1u);
    EXPECT_EQ(trimmed.getFilteredColumns().size(), 1u);
    EXPECT_TRUE(mlir::isa<mlir::storage::EdgeIDType>(
        mlir::cast<mlir::db::ColumnType>(trimmed.getFilteredColumns().front().getType()).getType()));
    EXPECT_EQ(trimmed.getMinHops(), 1u);
    EXPECT_EQ(trimmed.getMaxHops(), std::optional<uint64_t> {2});
    EXPECT_EQ(trimmed.getEdgeType(), std::optional<llvm::StringRef> {"KNOWS_WELL"});
    ASSERT_FALSE(trimmed.getHop().empty());
    EXPECT_EQ(trimmed.getHop().front().getNumArguments(), 3u);
    EXPECT_TRUE(mlir::isa<mlir::db::Yield>(trimmed.getHop().front().getTerminator()));
}

TEST_F(ExplorePathsDialectTest, pushDownMovesSeedPredicatesAboveAndKeepsEndPredicatesBelow) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(pushDownProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createPushDownFilters());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    ASSERT_TRUE(exploration);

    // The seed predicate now cuts the scan the exploration reads
    EXPECT_TRUE(mlir::isa<mlir::db::FilterOp>(exploration.getInputNodes().getDefiningOp()));

    // The end predicate still reads the exploration's end column
    size_t filtersBelow = 0;
    module->walk([&](mlir::db::FilterOp filter) {
        for (const mlir::Value column : filter.getColumnsToFilter()) {
            if (column.getDefiningOp() == exploration.getOperation()) {
                filtersBelow++;
                break;
            }
        }
    });
    EXPECT_EQ(filtersBelow, 1u);
}

TEST_F(ExplorePathsDialectTest, lowersToALoopOverThePathIterator) {
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(hopRegionProgram);
    ASSERT_TRUE(dbModule);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = lower(*dbModule, reader.getView());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*nlModule)));

    mlir::nl::ExplorePaths exploration;
    size_t forCount = 0;
    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::ExplorePaths found = mlir::dyn_cast<mlir::nl::ExplorePaths>(operation)) {
            exploration = found;
        } else if (mlir::isa<mlir::nl::For>(operation)) {
            forCount++;
        }
    });

    ASSERT_TRUE(exploration);
    EXPECT_EQ(forCount, 3u);
    EXPECT_EQ(exploration.getDirection(), mlir::storage::PathDirection::Both);
    EXPECT_EQ(exploration.getMinHops(), 0u);
    EXPECT_FALSE(exploration.getMaxHops().has_value());
    EXPECT_EQ(exploration.getColumnsToFilter().size(), 1u);

    // The loop binds seeds, ends, paths and the one carried chunk
    const auto iterator = mlir::cast<mlir::nl::IteratorType>(exploration.getResult().getType());
    ASSERT_EQ(iterator.getChunkTypes().size(), 4u);
    EXPECT_TRUE(mlir::isa<mlir::storage::PathRefType>(
        mlir::cast<mlir::nl::ChunkType>(iterator.getChunkTypes()[2]).getElementType()));

    // The hop region came along, ending in a yield of one mask
    ASSERT_FALSE(exploration.getHop().empty());
    mlir::Block& hop = exploration.getHop().front();
    EXPECT_EQ(hop.getNumArguments(), 3u);
    auto yield = mlir::dyn_cast<mlir::nl::Yield>(hop.getTerminator());
    ASSERT_TRUE(yield);
    EXPECT_EQ(yield.getColumns().size(), 1u);
}

TEST_F(ExplorePathsSimpleGraphTest, unmatchableTypeEmitsTheZeroLengthRowsOnly) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    RowSink sink;
    runProgram(unmatchableTypeProgram, reader.getView(), sink);

    const Rows& rows = sink.rows();
    EXPECT_EQ(rows.size(), 8u);
    for (const Row& row : rows) {
        ASSERT_EQ(row.size(), 3u);
        EXPECT_EQ(row[0], row[2]);
        EXPECT_EQ(row[1], "[]");
    }
}

TEST_F(ExplorePathsSimpleGraphTest, walksPersonToPersonPathsAsTheOracle) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
        RowSink sink;
        runProgram(personToPersonProgram, reader.getView(), sink, chunkSize);

        Rows rows;
        sink.sortedRows(rows);

        // variable-length-paths-0.json with the names as node IDs: Remy is 0, Adam is 1
        Rows expected {
            {"0", "[0]", "1", "1"},
            {"1", "[4]", "0", "1"},
            {"0", "[0, 4]", "0", "2"},
            {"0", "[1, 7]", "0", "2"},
            {"1", "[4, 0]", "1", "2"},
            {"0", "[1, 7, 0]", "1", "3"},
            {"1", "[4, 1, 7]", "0", "3"},
            {"0", "[0, 4, 1, 7]", "0", "4"},
            {"0", "[1, 7, 0, 4]", "0", "4"},
            {"1", "[4, 1, 7, 0]", "1", "4"},
        };
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ExplorePathsSimpleGraphTest, hopPredicateCutsTheSubtreeUnderAFailingHop) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    RowSink sink;
    runProgram(personHopsProgram, reader.getView(), sink);

    Rows rows;
    sink.sortedRows(rows);

    // Remy->Ghosts fails the hop, so nothing goes through Ghosts: only the 2-cycle
    // between Remy and Adam remains, cut by the trail rule after two hops
    Rows expected {
        {"0", "[0]", "1"},
        {"1", "[4]", "0"},
        {"0", "[0, 4]", "0"},
        {"1", "[4, 0]", "1"},
    };
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(rows, expected);
}

TEST_F(ExplorePathsSimpleGraphTest, expandsTheSourceAndEndNodeLists) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    RowSink sink;
    runProgram(groupVariablesProgram, reader.getView(), sink);

    Rows rows;
    sink.sortedRows(rows);

    // Every two-hop path leaving a Person: Remy through Adam and through Ghosts, Adam
    // through Remy; Remy is 0, Adam 1, Computers 2, Eighties 3, Bio 4, Cooking 5, Ghosts 6
    Rows expected {
        {"0", "[0, 1]", "[1, 0]", "0"},
        {"0", "[0, 1]", "[1, 4]", "4"},
        {"0", "[0, 1]", "[1, 5]", "5"},
        {"0", "[0, 6]", "[6, 0]", "0"},
        {"1", "[1, 0]", "[0, 1]", "1"},
        {"1", "[1, 0]", "[0, 6]", "6"},
        {"1", "[1, 0]", "[0, 2]", "2"},
        {"1", "[1, 0]", "[0, 3]", "3"},
    };
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(rows, expected);
}

TEST_F(ExplorePathsSimpleGraphTest, outputsAPathColumnAsItsEdgeList) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    for (const size_t chunkSize : {size_t {1}, ChunkConfig::CHUNK_SIZE}) {
        RowSink sink;
        runProgram(rawPathOutputProgram, reader.getView(), sink, chunkSize);

        Rows rows;
        sink.sortedRows(rows);

        // KNOWS_WELL edges: Remy->Adam (0), Adam->Remy (4), Ghosts->Remy (7); from the
        // Persons only the 2-cycle is reachable within two hops
        Rows expected {
            {"0", "[0]", "1"},
            {"0", "[0, 4]", "0"},
            {"1", "[4]", "0"},
            {"1", "[4, 0]", "1"},
        };
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
    }
}
