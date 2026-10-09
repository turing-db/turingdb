#include <gtest/gtest.h>

#include <memory>

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
#include "StorageDialect.h"

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

// MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i {name:'Cooking'})
// RETURN a, b, i as codegen leaves it: the walk ends anywhere and the name of the node one
// hop past its end is compared afterwards
const char* const oneHopPastProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 edge_types ["KNOWS_WELL"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4, %5, %6, %7, %8 = db.get_out_edges_by_type(%2, ["INTERESTED_IN"], {%1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %9 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %10 = db.constant("Cooking" : !storage.string)
  %11 = db.eq %9, %10 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %12:3 = db.filter(%11, {%8, %4, %7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%12#0, %12#1, %12#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a {name:'Ghosts'})-[:KNOWS_WELL*1..2]->(b)-[:KNOWS_WELL]->(c)-[:INTERESTED_IN]->
// (i {name:'Bio'}) RETURN a, b, c, i: the named node is two hops past the walk's end
const char* const twoHopsPastProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Ghosts" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 2 edge_types ["KNOWS_WELL"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %b, %e1, %t1, %c, %a1 = db.get_out_edges_by_type(%2, ["KNOWS_WELL"], {%1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %c2, %e2, %t2, %i, %a2, %b2 = db.get_out_edges_by_type(%c, ["INTERESTED_IN"], {%a1, %b}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %p = db.get_node_properties(%i, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant("Bio" : !storage.string)
  %m = db.eq %p, %k : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %f:4 = db.filter(%m, {%a2, %b2, %c2, %i}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%f#0, %f#1, %f#2, %f#3) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a {name:'Remy'})-[*1..2]->(b)<-[:INTERESTED_IN]-(p:Person) WHERE id(p) = 9
// RETURN a, b, p: the node past the walk is pinned by its id, and reached against the edge
const char* const pinnedIDPastProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 2 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %p, %e, %t, %b, %a = db.get_in_edges_by_type_and_label(%2, ["INTERESTED_IN"], ["Person"], {%1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %k = db.constant(9 : i64)
  %m = db.eq %p, %k : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %f:3 = db.filter(%m, {%a, %b, %p}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%f#0, %f#1, %f#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The walk's unfiltered ends are read past the filter, so the walk has to keep producing them
const char* const endsReadPastTheFilterProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 edge_types ["KNOWS_WELL"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4, %5, %6, %7, %8 = db.get_out_edges_by_type(%2, ["INTERESTED_IN"], {%1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %9 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %10 = db.constant("Cooking" : !storage.string)
  %11 = db.eq %9, %10 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %12:3 = db.filter(%11, {%8, %4, %7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%12#0, %12#1, %12#2, %2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The hop leaves from the walk's seed, not its end
const char* const hopFromTheSeedProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 edge_types ["KNOWS_WELL"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4, %5, %6, %7, %8 = db.get_out_edges_by_type(%1, ["INTERESTED_IN"], {%2}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %9 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %10 = db.constant("Ghosts" : !storage.string)
  %11 = db.eq %9, %10 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %12:3 = db.filter(%11, {%4, %8, %7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%12#0, %12#1, %12#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The property is read off the write buffer, which no scan of the graph covers
const char* const pendingReadProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 edge_types ["KNOWS_WELL"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4, %5, %6, %7, %8 = db.get_out_edges_by_type(%2, ["INTERESTED_IN"], {%1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %9 = db.get_node_properties(%7, "name") all_pending : (!db.column<!storage.node_id>) -> !db.column<none>
  %10 = db.constant("Cooking" : !storage.string)
  %11 = db.eq %9, %10 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %12:3 = db.filter(%11, {%8, %4, %7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%12#0, %12#1, %12#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The walk already ends on a set of its own
const char* const boundEndProgram = R"mlir(
func.func @main() {
  %s = db.scan_nodes_by_property_value("name", "Adam" : !storage.string) : !db.column<!storage.node_id>
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 edge_types ["KNOWS_WELL"] end_nodes %s : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4, %5, %6, %7, %8 = db.get_out_edges_by_type(%2, ["INTERESTED_IN"], {%1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %9 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %10 = db.constant("Cooking" : !storage.string)
  %11 = db.eq %9, %10 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %12:3 = db.filter(%11, {%8, %4, %7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%12#0, %12#1, %12#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// A filter on a node some hops past a walk's end, turned by the fuse_explore_end_set pass into
// the set of nodes the walk can end on: the filtered node's scan, walked back over those hops
class ExploreEndSetPastHopsTest : public ::testing::Test {
protected:
    ExploreEndSetPastHopsTest()
        : _graph(Graph::create())
    {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();

        SimpleGraph::createSimpleGraph(_graph.get());
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

    void runPass(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseExploreEndSet());
        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(module)));
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

    // The rows of the program as written and the rows once the pass has given the walk its end
    // set, which must be the same rows
    void expectSameRowsOncePassed(const char* program, Rows& rows) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            const mlir::OwningOpRef<mlir::ModuleOp> filtered = parse(program);
            ASSERT_TRUE(filtered);

            RowSink filteredSink;
            runModule(*filtered, view, filteredSink, chunkSize);

            const mlir::OwningOpRef<mlir::ModuleOp> fused = parse(program);
            ASSERT_TRUE(fused);
            runPass(*fused);
            ASSERT_TRUE(findExplorePaths(*fused).getEndNodes());

            RowSink fusedSink;
            runModule(*fused, view, fusedSink, chunkSize);

            Rows expected;
            filteredSink.sortedRows(expected);

            Rows actual;
            fusedSink.sortedRows(actual);

            EXPECT_EQ(actual, expected) << "chunk size " << chunkSize;
            rows = actual;
        }
    }

    mlir::MLIRContext _context;
    std::unique_ptr<Graph> _graph;
};

TEST_F(ExploreEndSetPastHopsTest, endsTheWalkOnTheNodesOneHopFromTheNamedOne) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(oneHopPastProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    ASSERT_TRUE(exploration);
    ASSERT_TRUE(exploration.getEndNodes());

    // Cooking's INTERESTED_IN predecessors, the hop of the pattern walked against its edges
    mlir::db::GetInEdgesByType reversed = exploration.getEndNodes().getDefiningOp<mlir::db::GetInEdgesByType>();
    ASSERT_TRUE(reversed);
    EXPECT_EQ(exploration.getEndNodes(), reversed.getSrcids());
    ASSERT_EQ(reversed.getEdgeTypes().size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(reversed.getEdgeTypes()[0]).getValue(), "INTERESTED_IN");

    mlir::db::ScanNodesByPropertyValue named = reversed.getInputNodes().getDefiningOp<mlir::db::ScanNodesByPropertyValue>();
    ASSERT_TRUE(named);
    EXPECT_EQ(named.getProperty(), "name");
    EXPECT_EQ(mlir::dyn_cast<mlir::StringAttr>(named.getValue()).getValue(), "Cooking");

    // The set is built ahead of the seeds, so lowering fills it before the walk runs
    EXPECT_EQ(&named->getBlock()->front(), named.getOperation());

    // The walk's end is not the named node, so the hop and its filter still bind it
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);
}

TEST_F(ExploreEndSetPastHopsTest, walksBackEveryHopToTheWalk) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(twoHopsPastProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    ASSERT_TRUE(exploration);
    ASSERT_TRUE(exploration.getEndNodes());

    mlir::db::GetInEdgesByType knows = exploration.getEndNodes().getDefiningOp<mlir::db::GetInEdgesByType>();
    ASSERT_TRUE(knows);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(knows.getEdgeTypes()[0]).getValue(), "KNOWS_WELL");

    // Each node is walked back from once, however many of the next hop's nodes reach it
    mlir::db::RemoveDuplicates distinct = knows.getInputNodes().getDefiningOp<mlir::db::RemoveDuplicates>();
    ASSERT_TRUE(distinct);

    mlir::db::GetInEdgesByType interested = distinct.getColumns()[0].getDefiningOp<mlir::db::GetInEdgesByType>();
    ASSERT_TRUE(interested);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>(interested.getEdgeTypes()[0]).getValue(), "INTERESTED_IN");
    EXPECT_TRUE(interested.getInputNodes().getDefiningOp<mlir::db::ScanNodesByPropertyValue>());
}

TEST_F(ExploreEndSetPastHopsTest, scansTheNodesAnIDPins) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(pinnedIDPastProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    ASSERT_TRUE(exploration);
    ASSERT_TRUE(exploration.getEndNodes());

    // Against an in-edge hop the walk back follows the edge forward
    mlir::db::GetOutEdgesByType reversed = exploration.getEndNodes().getDefiningOp<mlir::db::GetOutEdgesByType>();
    ASSERT_TRUE(reversed);
    EXPECT_EQ(exploration.getEndNodes(), reversed.getTgtids());

    // The hop asked its node for Person, so the pinned node is kept only if it is one
    mlir::db::FilterOp labelled = reversed.getInputNodes().getDefiningOp<mlir::db::FilterOp>();
    ASSERT_TRUE(labelled);
    mlir::db::ConstScanNodes pinned = labelled.getColumnsToFilter()[0].getDefiningOp<mlir::db::ConstScanNodes>();
    ASSERT_TRUE(pinned);
    ASSERT_EQ(pinned.getNodeIDs().size(), 1u);
    EXPECT_EQ(pinned.getNodeIDs()[0], 9);
}

TEST_F(ExploreEndSetPastHopsTest, emitsTheRowsOfTheFilteredWalk) {
    // Remy (0) reaches Adam (1), who is interested in Cooking (5)
    Rows rows;
    expectSameRowsOncePassed(oneHopPastProgram, rows);
    EXPECT_EQ(rows, (Rows {{"0", "1", "5"}}));

    // Ghosts (6) reaches Remy (0), who knows Adam (1), who is interested in Bio (4)
    expectSameRowsOncePassed(twoHopsPastProgram, rows);
    EXPECT_EQ(rows, (Rows {{"6", "0", "1", "4"}}));

    // Remy (0) reaches Computers (2), which Luc (9) is interested in
    expectSameRowsOncePassed(pinnedIDPastProgram, rows);
    EXPECT_EQ(rows, (Rows {{"0", "2", "9"}}));
}

TEST_F(ExploreEndSetPastHopsTest, leavesShapesItCannotTake) {
    for (const char* program : {endsReadPastTheFilterProgram, hopFromTheSeedProgram, pendingReadProgram}) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module) << program;

        runPass(*module);

        mlir::db::ExplorePaths left = findExplorePaths(*module);
        ASSERT_TRUE(left);
        EXPECT_FALSE(left.getEndNodes()) << program;
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u) << program;
        EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 1u) << program;
    }
}

TEST_F(ExploreEndSetPastHopsTest, keepsAnEndSetTheWalkHasAlready) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(boundEndProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths exploration = findExplorePaths(*module);
    ASSERT_TRUE(exploration);
    ASSERT_TRUE(exploration.getEndNodes());
    EXPECT_TRUE(exploration.getEndNodes().getDefiningOp<mlir::db::ScanNodesByPropertyValue>());
    EXPECT_EQ(countOps<mlir::db::GetInEdgesByType>(*module), 0u);
}
