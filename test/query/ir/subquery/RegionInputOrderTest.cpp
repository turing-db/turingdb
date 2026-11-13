#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "SimpleGraph.h"
#include "StorageDialect.h"
#include "TuringTest.h"

#include "IRTestRows.h"

using namespace db;
using namespace turing::test;

// An op with a body takes the columns in flight as inputs, in no order the rows depend on. A
// constant among them holds one value for every row, so the rows the op answers for are those
// of an input holding rows, wherever that input stands. Remy and Adam each know the other, and
// are the only people knowing someone older than 30.
class RegionInputOrderTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void expectRows(const std::string& ops, const Rows& expected) {
        std::string program = R"mlir(
!nodes = !db.column<!storage.node_id>
!edges = !db.column<!storage.edge_id>
!etypes = !db.column<!storage.edge_type_id>
!tags = !db.column<ui64>
!ints = !db.column<i64>
!ages = !db.column<!storage.nullable<i64>>
!names = !db.column<!storage.nullable<!storage.string>>
!bools = !db.column<!storage.bool>
!counts = !db.column<ui64>
!unresolved = !db.column<none>

func.func @main() {
  %k = db.constant(30 : i64)
  %n = db.scan_nodes_by_label(["Person"]) : !nodes
  %name = db.get_node_properties(%n, "name") : (!nodes) -> !names
)mlir";

        program += ops;
        program += "\n  return\n}\n";

        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module) << program;

        const mlir::func::FuncOp dbFunction = module->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        RowSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();

        EXPECT_EQ(sink.rows(), expected) << program;
    }

    std::unique_ptr<Graph> _graph;
    mlir::MLIRContext _context;
};

TEST_F(RegionInputOrderTest, decidesAnExistsCarryingTheScopeOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %b = db.exists_subquery(%k, %n) carries_scope {
  ^bb0(%kIn: !ints, %nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %mTag = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kIn : (!ages, !ints) -> !bools
    %kept = db.filter(%older, {%mTag}) : (!bools, !tags) -> !tags
    db.exists_yield %kept, {}
  } : (!ints, !nodes) -> !bools
  db.output(%name, %b) : !names, !bools)mlir",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}});
}

TEST_F(RegionInputOrderTest, decidesAnExistsRunPerRowOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %b = db.exists_subquery(%k, %n) {
  ^bb0(%kIn: !ints, %nIn: !nodes):
    %kRow = db.row_constant %kIn : <i64>
    %row = db.row_constant %nIn : <!storage.node_id>
    %s, %e, %et, %m = db.get_out_edges(%row, {}) : (!nodes) -> (!nodes, !edges, !etypes, !nodes)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kRow : (!ages, !ints) -> !bools
    %kept = db.filter(%older, {%m}) : (!bools, !nodes) -> !nodes
    %distinct = db.remove_duplicates(%kept) : (!nodes) -> !nodes
    db.exists_yield {%distinct} : {!nodes}
  } : (!ints, !nodes) -> !bools
  db.output(%name, %b) : !names, !bools)mlir",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}});
}

TEST_F(RegionInputOrderTest, countsCarryingTheScopeOverTheRowsOfTheSecondInput) {
    expectRows(R"mlir(
  %c = db.count_subquery(%k, %n) carries_scope {
  ^bb0(%kIn: !ints, %nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %mTag = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kIn : (!ages, !ints) -> !bools
    %kept = db.filter(%older, {%mTag}) : (!bools, !tags) -> !tags
    db.count_subquery_yield %kept, {}
  } : (!ints, !nodes) -> !counts
  db.output(%name, %c) : !names, !counts)mlir",
               {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(RegionInputOrderTest, countsPerRowOverTheRowsOfTheSecondInput) {
    expectRows(R"mlir(
  %c = db.count_subquery(%k, %n) {
  ^bb0(%kIn: !ints, %nIn: !nodes):
    %kRow = db.row_constant %kIn : <i64>
    %row = db.row_constant %nIn : <!storage.node_id>
    %s, %e, %et, %m = db.get_out_edges(%row, {}) : (!nodes) -> (!nodes, !edges, !etypes, !nodes)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kRow : (!ages, !ints) -> !bools
    %kept = db.filter(%older, {%m}) : (!bools, !nodes) -> !nodes
    %distinct = db.remove_duplicates(%kept) : (!nodes) -> !nodes
    db.count_subquery_yield {%distinct} : {!nodes}
  } : (!ints, !nodes) -> !counts
  db.output(%name, %c) : !names, !counts)mlir",
               {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(RegionInputOrderTest, collectsCarryingTheScopeOverTheRowsOfTheSecondInput) {
    expectRows(R"mlir(
  %l = db.collect_subquery(%k, %n) carries_scope {
  ^bb0(%kIn: !ints, %nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %mTag = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kIn : (!ages, !ints) -> !bools
    %kept:2 = db.filter(%older, {%mTag, %m}) : (!bools, !tags, !nodes) -> (!tags, !nodes)
    %mName = db.get_node_properties(%kept#1, "name") : (!nodes) -> !names
    db.collect_subquery_yield tag %kept#0, %mName, {} : !names
  } : (!ints, !nodes) -> !unresolved
  db.output(%name, %l) : !names, !unresolved)mlir",
               {{"Remy", "[Adam]"}, {"Adam", "[Remy]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(RegionInputOrderTest, collectsPerRowOverTheRowsOfTheSecondInput) {
    expectRows(R"mlir(
  %l = db.collect_subquery(%k, %n) {
  ^bb0(%kIn: !ints, %nIn: !nodes):
    %kRow = db.row_constant %kIn : <i64>
    %row = db.row_constant %nIn : <!storage.node_id>
    %s, %e, %et, %m = db.get_out_edges(%row, {}) : (!nodes) -> (!nodes, !edges, !etypes, !nodes)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kRow : (!ages, !ints) -> !bools
    %kept = db.filter(%older, {%m}) : (!bools, !nodes) -> !nodes
    %mName = db.get_node_properties(%kept, "name") : (!nodes) -> !names
    %distinct = db.remove_duplicates(%mName) : (!names) -> !names
    db.collect_subquery_yield %distinct, {%distinct} : !names, {!names}
  } : (!ints, !nodes) -> !unresolved
  db.output(%name, %l) : !names, !unresolved)mlir",
               {{"Remy", "[Adam]"}, {"Adam", "[Remy]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(RegionInputOrderTest, buildsAPatternComprehensionOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %l = db.pattern_comprehension(%k, %n) {
  ^bb0(%kIn: !ints, %nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %mTag = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kIn : (!ages, !ints) -> !bools
    %kept:2 = db.filter(%older, {%mTag, %m}) : (!bools, !tags, !nodes) -> (!tags, !nodes)
    %mName = db.get_node_properties(%kept#1, "name") : (!nodes) -> !names
    db.comprehension_yield %kept#0, %mName : !names
  } : (!ints, !nodes) -> !unresolved
  db.output(%name, %l) : !names, !unresolved)mlir",
               {{"Remy", "[Adam]"}, {"Adam", "[Remy]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(RegionInputOrderTest, padsAnOptionalMatchOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %kOut, %nOut, %mOut = db.optional_match(%k, %n) {
  ^bb0(%kIn: !ints, %nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %tagc = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kIn : (!ages, !ints) -> !bools
    %kept:3 = db.filter(%older, {%s, %tagc, %m}) : (!bools, !nodes, !tags, !nodes) -> (!nodes, !tags, !nodes)
    db.optional_yield %kept#1, {%kIn, %kept#0, %kept#2} : {!ints, !nodes, !nodes}
  } : (!ints, !nodes) -> (!ints, !nodes, !nodes)
  %nName = db.get_node_properties(%nOut, "name") : (!nodes) -> !names
  %mName = db.get_node_properties(%mOut, "name") : (!nodes) -> !names
  db.output(%nName, %mName) : !names, !names)mlir",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"},
                {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(RegionInputOrderTest, callsABodyCarryingTheScopeOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %kOut, %nOut, %mOut = db.call_subquery(%k, %n) carries_scope {
  ^bb0(%kIn: !ints, %nIn: !nodes):
    %s, %e, %et, %m = db.get_out_edges(%nIn, {}) : (!nodes) -> (!nodes, !edges, !etypes, !nodes)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kIn : (!ages, !ints) -> !bools
    %kept:2 = db.filter(%older, {%s, %m}) : (!bools, !nodes, !nodes) -> (!nodes, !nodes)
    db.subquery_yield {%kIn, %kept#0, %kept#1} : {!ints, !nodes, !nodes}
  } : (!ints, !nodes) -> (!ints, !nodes, !nodes)
  %nName = db.get_node_properties(%nOut, "name") : (!nodes) -> !names
  %mName = db.get_node_properties(%mOut, "name") : (!nodes) -> !names
  db.output(%nName, %mName) : !names, !names)mlir",
               {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

TEST_F(RegionInputOrderTest, callsABodyPerRowOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %kOut, %nOut, %c = db.call_subquery(%k, %n) {
  ^bb0(%kIn: !ints, %nIn: !nodes):
    %kRow = db.row_constant %kIn : <i64>
    %row = db.row_constant %nIn : <!storage.node_id>
    %s, %e, %et, %m = db.get_out_edges(%row, {}) : (!nodes) -> (!nodes, !edges, !etypes, !nodes)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kRow : (!ages, !ints) -> !bools
    %kept = db.filter(%older, {%m}) : (!bools, !nodes) -> !nodes
    %count = db.count(%kept) : (!nodes) -> !unresolved
    db.subquery_yield {%count} : {!unresolved}
  } : (!ints, !nodes) -> (!ints, !nodes, !unresolved)
  %nName = db.get_node_properties(%nOut, "name") : (!nodes) -> !names
  db.output(%nName, %c) : !names, !unresolved)mlir",
               {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(RegionInputOrderTest, padsAnOptionalCallCarryingTheScopeOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %kOut, %nOut, %mOut = db.call_subquery(%k, %n) carries_scope optional {
  ^bb0(%kIn: !ints, %nIn: !nodes, %tag: !tags):
    %s, %e, %et, %m, %tagc = db.get_out_edges(%nIn, {%tag}) : (!nodes, !tags) -> (!nodes, !edges, !etypes, !nodes, !tags)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kIn : (!ages, !ints) -> !bools
    %kept:3 = db.filter(%older, {%s, %tagc, %m}) : (!bools, !nodes, !tags, !nodes) -> (!nodes, !tags, !nodes)
    db.subquery_yield %kept#1, {%kIn, %kept#0, %kept#2} : {!ints, !nodes, !nodes}
  } : (!ints, !nodes) -> (!ints, !nodes, !nodes)
  %nName = db.get_node_properties(%nOut, "name") : (!nodes) -> !names
  %mName = db.get_node_properties(%mOut, "name") : (!nodes) -> !names
  db.output(%nName, %mName) : !names, !names)mlir",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"},
                {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}

TEST_F(RegionInputOrderTest, padsAnOptionalCallPerRowOverTheRowsOfItsSecondInput) {
    expectRows(R"mlir(
  %kOut, %nOut, %mOut = db.call_subquery(%k, %n) optional {
  ^bb0(%kIn: !ints, %nIn: !nodes):
    %kRow = db.row_constant %kIn : <i64>
    %row = db.row_constant %nIn : <!storage.node_id>
    %s, %e, %et, %m = db.get_out_edges(%row, {}) : (!nodes) -> (!nodes, !edges, !etypes, !nodes)
    %age = db.get_node_properties(%m, "age") : (!nodes) -> !ages
    %older = db.gt %age, %kRow : (!ages, !ints) -> !bools
    %kept = db.filter(%older, {%m}) : (!bools, !nodes) -> !nodes
    %one = db.limit(%kept) count 1 : (!nodes) -> !nodes
    db.subquery_yield {%one} : {!nodes}
  } : (!ints, !nodes) -> (!ints, !nodes, !nodes)
  %nName = db.get_node_properties(%nOut, "name") : (!nodes) -> !names
  %mName = db.get_node_properties(%mOut, "name") : (!nodes) -> !names
  db.output(%nName, %mName) : !names, !names)mlir",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"},
                {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});
}
