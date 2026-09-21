#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
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
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "StorageDialect.h"
#include "StringRowSink.h"

#include "Graph.h"
#include "LocalMemory.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// MATCH (a {name:'Remy'})-[e*1..3]->(b {name:'Adam'}) RETURN a, b as codegen leaves it: the
// walk ends wherever it can and the name of each end is compared afterwards
const char* const pinnedEndProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4 = db.get_node_properties(%2, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Adam" : !storage.string)
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %7:2 = db.filter(%6, {%2, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%7#1, %7#0) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same with the literal on the left of the equality
const char* const swappedPinnedEndProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4 = db.get_node_properties(%2, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Adam" : !storage.string)
  %6 = db.eq %5, %4 : (!db.column<!storage.string>, !db.column<none>) -> !db.column<!storage.bool>
  %7:2 = db.filter(%6, {%2, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%7#1, %7#0) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a:Person {name:'Remy'})-[e*1..3]->(b:Person {name:'Adam'}) RETURN a, b once the
// end label has been fused into the exploration
const char* const labelledPinnedEndProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string, ["Person"]) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 end_labels ["Person"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4 = db.get_node_properties(%2, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Adam" : !storage.string)
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %7:2 = db.filter(%6, {%2, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%7#1, %7#0) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a {name:'Remy'})-[e*1..3]->(b {name:'Adam'}) WHERE b.age = a.age RETURN a, b: the
// name is a set to head for, the age a comparison between the two ends that stays a filter
const char* const correlatedEndProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4 = db.get_node_properties(%2, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Adam" : !storage.string)
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %7:2 = db.filter(%6, {%2, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %8 = db.get_node_properties(%7#0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %9 = db.get_node_properties(%7#1, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %10 = db.eq %8, %9 : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
  %11:2 = db.filter(%10, {%7#0, %7#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%11#1, %11#0) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The property compared is the seed's, not the end's
const char* const seedPropertyProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Adam" : !storage.string)
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %7:2 = db.filter(%6, {%2, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%7#1, %7#0) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The unfiltered ends are read past the filter, so the walk has to keep producing them
const char* const sharedEndProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4 = db.get_node_properties(%2, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Adam" : !storage.string)
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %7:2 = db.filter(%6, {%2, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%7#1, %7#0, %2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The end is bound to a carried column already
const char* const boundEndProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %p = db.get_node_properties(%0#1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %c = db.constant("Adam" : !storage.string)
  %m = db.eq %p, %c : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %1:2 = db.filter(%m, {%0#0, %0#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%1#0, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The property is read off the write buffer, which no scan of the graph covers
const char* const pendingReadProgram = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %1, %2, %3 = db.explore_paths(%0, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %4 = db.get_node_properties(%2, "name") all_pending : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Adam" : !storage.string)
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %7:2 = db.filter(%6, {%2, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%7#1, %7#0) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// The filter comparing a walk's end nodes against a literal, fused into a scan of the nodes
// holding that literal and an end set on the walk
class ExploreEndPropertyTest : public ::testing::Test {
protected:
    ExploreEndPropertyTest() {
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

    void runPass(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseExploreEndSet());
        ASSERT_TRUE(mlir::succeeded(passManager.run(module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(module)));
    }

    mlir::OwningOpRef<mlir::ModuleOp> lower(mlir::ModuleOp dbModule, const GraphView& view) {
        const mlir::func::FuncOp dbFunction = dbModule.lookupSymbol<mlir::func::FuncOp>("main");
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));

        DBLowering lowering(&_context, &view);
        lowering.lower(dbFunction, *nlModule);

        return nlModule;
    }

    void runModule(mlir::ModuleOp dbModule, const GraphView& view, RowSink& sink, size_t chunkSize) {
        const mlir::OwningOpRef<mlir::ModuleOp> nlModule = lower(dbModule, view);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    // The rows of the program as written, and the rows of the program once the pass has fused
    // its end filter, which must be the same rows
    void expectSameRowsOncePassed(const char* program, const GraphView& view, Rows& rows) {
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

            EXPECT_FALSE(expected.empty()) << "chunk size " << chunkSize;
            EXPECT_EQ(actual, expected) << "chunk size " << chunkSize;

            rows = actual;
        }
    }

    mlir::MLIRContext _context;
};

TEST_F(ExploreEndPropertyTest, fusesThePropertyFilterIntoAnEndSet) {
    for (const char* program : {pinnedEndProgram, swappedPinnedEndProgram}) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module) << program;

        runPass(*module);

        mlir::db::ExplorePaths fused = findExplorePaths(*module);
        ASSERT_TRUE(fused);
        ASSERT_TRUE(fused.getEndNodes()) << program;

        mlir::db::ScanNodesByPropertyValue set = fused.getEndNodes().getDefiningOp<mlir::db::ScanNodesByPropertyValue>();
        ASSERT_TRUE(set) << program;
        EXPECT_EQ(set.getProperty(), "name");
        EXPECT_EQ(mlir::dyn_cast<mlir::StringAttr>(set.getValue()).getValue(), "Adam");
        EXPECT_FALSE(set.getLabels().has_value());

        // The set is scanned ahead of the seeds, so lowering fills it before the walk runs
        EXPECT_EQ(&set->getBlock()->front(), set.getOperation());

        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u) << program;
        EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 0u) << program;
        EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 0u) << program;
        EXPECT_EQ(countOps<mlir::db::ConstantOp>(*module), 0u) << program;
    }
}

TEST_F(ExploreEndPropertyTest, foldsTheEndLabelsIntoTheSet) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(labelledPinnedEndProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    ASSERT_TRUE(fused.getEndNodes());
    EXPECT_FALSE(fused.getEndLabels().has_value());

    mlir::db::ScanNodesByPropertyValue set = fused.getEndNodes().getDefiningOp<mlir::db::ScanNodesByPropertyValue>();
    ASSERT_TRUE(set);
    ASSERT_TRUE(set.getLabels().has_value());
    ASSERT_EQ(set.getLabels()->size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>((*set.getLabels())[0]).getValue(), "Person");
}

TEST_F(ExploreEndPropertyTest, fusesTheLiteralAndLeavesTheComparisonBetweenEnds) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(correlatedEndProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    EXPECT_TRUE(fused.getEndNodes());

    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::EqOp>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 2u);
    EXPECT_EQ(countOps<mlir::db::ConstantOp>(*module), 0u);
}

TEST_F(ExploreEndPropertyTest, leavesShapesItCannotTake) {
    for (const char* program : {seedPropertyProgram, sharedEndProgram, boundEndProgram, pendingReadProgram}) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module) << program;

        runPass(*module);

        mlir::db::ExplorePaths left = findExplorePaths(*module);
        ASSERT_TRUE(left);
        EXPECT_FALSE(left.getEndNodes()) << program;
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u) << program;
        EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module) + countOps<mlir::db::ScanNodesByLabel>(*module), 1u) << program;
    }
}

class ExploreEndPropertySimpleGraphTest : public ExploreEndPropertyTest {
protected:
    ExploreEndPropertySimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(ExploreEndPropertySimpleGraphTest, emitsTheRowsOfTheFilteredWalk) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // Remy (0) reaches Adam (1) directly and through Ghosts (6) and back: 0->1 and 0->6->0->1
    Rows rows;
    expectSameRowsOncePassed(pinnedEndProgram, view, rows);
    EXPECT_EQ(rows, (Rows {{"0", "1"}, {"0", "1"}}));

    expectSameRowsOncePassed(labelledPinnedEndProgram, view, rows);
    EXPECT_EQ(rows, (Rows {{"0", "1"}, {"0", "1"}}));
}

// The same query through the whole engine: codegen, the pass pipeline, lowering and the
// interpreter, over the simpledb graph
class ExploreEndPropertyCypherTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    std::vector<StringRowSink::Row> run(std::string_view query) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        return rows;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ExploreEndPropertyCypherTest, walksToThePinnedEnd) {
    using Rows = std::vector<StringRowSink::Row>;

    EXPECT_EQ(run("MATCH (a {name:'Remy'})-[e*1..3]->(b {name:'Adam'}) RETURN a, b"), (Rows {{"0", "1"}, {"0", "1"}}));
    EXPECT_EQ(run("MATCH (a:Person {name:'Remy'})-[e*1..3]->(b:Person {name:'Adam'}) RETURN a, b"), (Rows {{"0", "1"}, {"0", "1"}}));
    EXPECT_EQ(run("MATCH (a:Person {name:'Remy'})-[e*1..3]->(b:Person {name:'Nobody'}) RETURN a, b"), Rows {});
}
