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

// MATCH (a {name:'Remy'}), (b {name:'Adam'}) WITH a, b MATCH (a)-[e*1..3]->(b) RETURN a, b
// once fuse_explore_end_nodes has bound the end: one walk per row of the product
const char* const pairedEndProgram = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %5 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
    db.yield %5 : !db.column<!storage.node_id>
  } factor {
    %5 = db.scan_nodes_by_property_value("name", "Adam" : !storage.string) : !db.column<!storage.node_id>
    db.yield %5 : !db.column<!storage.node_id>
  }
  %1, %2, %3, %4 = db.explore_paths(%0#0, {%0#1}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%1, %4) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a {name:'Remy'})-->(c), (b {name:'Adam'}) WITH a, b, c MATCH (a)-[e*1..3]->(b)
// RETURN a, b, c: the seed's factor carries a hop the walk keeps carrying
const char* const carriedSeedProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %6 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
    %7, %8, %9, %10 = db.get_out_edges(%6, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %7, %10 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %6 = db.scan_nodes_by_property_value("name", "Adam" : !storage.string) : !db.column<!storage.node_id>
    db.yield %6 : !db.column<!storage.node_id>
  }
  %1, %2, %3, %4, %5 = db.explore_paths(%0#0, {%0#2, %0#1}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%1, %4, %5) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a {name:'Remy'}), (b {name:'Adam'}), (c:Interest) WITH a, b, c MATCH (a)-[e*1..3]->(b)
// RETURN a, b, c: the end sits in a product nested inside the factor the seeds are crossed with
const char* const nestedEndProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %6 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
    db.yield %6 : !db.column<!storage.node_id>
  } factor {
    %6:2 = db.cross_product factor {
      %7 = db.scan_nodes_by_property_value("name", "Adam" : !storage.string) : !db.column<!storage.node_id>
      db.yield %7 : !db.column<!storage.node_id>
    } factor {
      %7 = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
      db.yield %7 : !db.column<!storage.node_id>
    }
    db.yield %6#0, %6#1 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  }
  %1, %2, %3, %4, %5 = db.explore_paths(%0#0, {%0#1, %0#2}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%1, %4, %5) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The end is a hop off the seed, in the seed's own factor: it differs row by row
const char* const correlatedEndProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %6 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
    %7, %8, %9, %10 = db.get_out_edges(%6, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %7, %10 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %6 = db.scan_nodes_by_label(["Interest"]) : !db.column<!storage.node_id>
    db.yield %6 : !db.column<!storage.node_id>
  }
  %1, %2, %3, %4, %5 = db.explore_paths(%0#0, {%0#1, %0#2}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%1, %4, %5) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The product's rows are read past the walk
const char* const sharedProductProgram = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %5 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
    db.yield %5 : !db.column<!storage.node_id>
  } factor {
    %5 = db.scan_nodes_by_property_value("name", "Adam" : !storage.string) : !db.column<!storage.node_id>
    db.yield %5 : !db.column<!storage.node_id>
  }
  %1, %2, %3, %4 = db.explore_paths(%0#0, {%0#1}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%1, %4, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The end's factor yields a hop beside it, which the set could not carry
const char* const wideEndFactorProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %6 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
    db.yield %6 : !db.column<!storage.node_id>
  } factor {
    %6 = db.scan_nodes_by_property_value("name", "Adam" : !storage.string) : !db.column<!storage.node_id>
    %7, %8, %9, %10 = db.get_out_edges(%6, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %7, %10 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  }
  %1, %2, %3, %4, %5 = db.explore_paths(%0#0, {%0#1, %0#2}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%1, %4, %5) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The seeds come from a hop, not a product: MATCH (a)-->(b) MATCH (a)-[e*1..3]->(b)
const char* const hopSeedProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 to 3 end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%0#0, %0#3) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// The end column a walk is bound to, drawn from a factor of the cross product its seeds come
// from, turned into the set of ends the walk heads for
class ExploreEndFactorTest : public ::testing::Test {
protected:
    ExploreEndFactorTest() {
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

    // The scan of the Adam node the set comes from, standing first in the function
    static void expectSetIsTheAdamScan(mlir::db::ExplorePaths fused) {
        ASSERT_TRUE(fused.getEndNodes());
        EXPECT_FALSE(fused.getEndColumn().has_value());

        mlir::db::ScanNodesByPropertyValue set = fused.getEndNodes().getDefiningOp<mlir::db::ScanNodesByPropertyValue>();
        ASSERT_TRUE(set);
        EXPECT_EQ(mlir::dyn_cast<mlir::StringAttr>(set.getValue()).getValue(), "Adam");
        EXPECT_EQ(&set->getBlock()->front(), set.getOperation());
        EXPECT_TRUE(mlir::isa<mlir::func::FuncOp>(set->getParentOp()));
    }

    void runPass(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createFuseExploreEndFactor());
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

    // The rows of the program as written, and once the pass has turned its end column into
    // a set, which must be the same rows
    void expectSameRowsOncePassed(const char* program, const GraphView& view, Rows& rows) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            const mlir::OwningOpRef<mlir::ModuleOp> paired = parse(program);
            ASSERT_TRUE(paired);

            RowSink pairedSink;
            runModule(*paired, view, pairedSink, chunkSize);

            const mlir::OwningOpRef<mlir::ModuleOp> fused = parse(program);
            ASSERT_TRUE(fused);
            runPass(*fused);
            ASSERT_TRUE(findExplorePaths(*fused).getEndNodes());

            RowSink fusedSink;
            runModule(*fused, view, fusedSink, chunkSize);

            Rows expected;
            pairedSink.sortedRows(expected);

            Rows actual;
            fusedSink.sortedRows(actual);

            EXPECT_FALSE(expected.empty()) << "chunk size " << chunkSize;
            EXPECT_EQ(actual, expected) << "chunk size " << chunkSize;

            rows = actual;
        }
    }

    mlir::MLIRContext _context;
};

TEST_F(ExploreEndFactorTest, turnsTheOtherFactorIntoTheEndSet) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(pairedEndProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    expectSetIsTheAdamScan(fused);
    EXPECT_EQ(fused.getColumnsToFilter().size(), 0u);
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);

    // The walk's seeds are the Remy scan, and its output reads the end off the walk
    EXPECT_TRUE(fused.getInputNodes().getDefiningOp<mlir::db::ScanNodesByPropertyValue>());

    mlir::db::Output output;
    (*module)->walk([&output](mlir::db::Output op) {
        output = op;
    });
    ASSERT_TRUE(output);
    EXPECT_EQ(output->getOperand(0), fused.getSrcids());
    EXPECT_EQ(output->getOperand(1), fused.getTgtids());
}

TEST_F(ExploreEndFactorTest, keepsCarryingWhatTheSeedFactorYields) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(carriedSeedProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    expectSetIsTheAdamScan(fused);
    ASSERT_EQ(fused.getColumnsToFilter().size(), 1u);
    EXPECT_TRUE(fused.getColumnsToFilter()[0].getDefiningOp<mlir::db::GetOutEdges>());
    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
}

TEST_F(ExploreEndFactorTest, peelsTheEndOutOfANestedProduct) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(nestedEndProgram);
    ASSERT_TRUE(module);

    runPass(*module);

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    expectSetIsTheAdamScan(fused);
    ASSERT_EQ(fused.getColumnsToFilter().size(), 1u);

    // Remy crossed with the Interests is what remains, and the walk carries the Interest
    ASSERT_EQ(countOps<mlir::db::CrossProduct>(*module), 1u);
    mlir::db::CrossProduct remaining = fused.getInputNodes().getDefiningOp<mlir::db::CrossProduct>();
    ASSERT_TRUE(remaining);
    EXPECT_EQ(remaining.getNumResults(), 2u);
    EXPECT_EQ(fused.getColumnsToFilter()[0], remaining.getResult(1));
    EXPECT_EQ(countOps<mlir::db::ScanNodesByLabel>(*module), 1u);
}

TEST_F(ExploreEndFactorTest, leavesShapesItCannotTake) {
    for (const char* program : {correlatedEndProgram, sharedProductProgram, wideEndFactorProgram, hopSeedProgram}) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module) << program;

        const size_t products = countOps<mlir::db::CrossProduct>(*module);
        runPass(*module);

        mlir::db::ExplorePaths left = findExplorePaths(*module);
        ASSERT_TRUE(left);
        EXPECT_FALSE(left.getEndNodes()) << program;
        EXPECT_TRUE(left.getEndColumn().has_value()) << program;
        EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), products) << program;
    }
}

class ExploreEndFactorSimpleGraphTest : public ExploreEndFactorTest {
protected:
    ExploreEndFactorSimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(ExploreEndFactorSimpleGraphTest, emitsTheRowsOfThePairedWalk) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    // Remy (0) reaches Adam (1) directly and through Ghosts (6) and back: 0->1 and 0->6->0->1
    Rows rows;
    expectSameRowsOncePassed(pairedEndProgram, view, rows);
    EXPECT_EQ(rows, (Rows {{"0", "1"}, {"0", "1"}}));

    // Each of the two, once per node Remy points at: Adam (1), Computers (2), Eighties (3), Ghosts (6)
    expectSameRowsOncePassed(carriedSeedProgram, view, rows);
    EXPECT_EQ(rows, (Rows {{"0", "1", "1"}, {"0", "1", "1"}, {"0", "1", "2"}, {"0", "1", "2"}, {"0", "1", "3"}, {"0", "1", "3"}, {"0", "1", "6"}, {"0", "1", "6"}}));

    expectSameRowsOncePassed(nestedEndProgram, view, rows);
    for (const Row& row : rows) {
        EXPECT_EQ(row[0], "0");
        EXPECT_EQ(row[1], "1");
    }
}

// The same queries through the whole engine over simpledb
class ExploreEndFactorCypherTest : public TuringTest {
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

TEST_F(ExploreEndFactorCypherTest, walksToTheCrossedEnd) {
    using Rows = std::vector<StringRowSink::Row>;

    EXPECT_EQ(run("MATCH (a {name:'Remy'}), (b {name:'Adam'}) WITH a, b MATCH (a)-[e*1..3]->(b) RETURN a, b"),
              (Rows {{"0", "1"}, {"0", "1"}}));

    EXPECT_EQ(run("MATCH (a {name:'Remy'})-->(c), (b {name:'Adam'}) WITH a, b, c MATCH (a)-[e*1..3]->(b) RETURN a, b, c"),
              (Rows {{"0", "1", "1"}, {"0", "1", "1"}, {"0", "1", "2"}, {"0", "1", "2"}, {"0", "1", "3"}, {"0", "1", "3"}, {"0", "1", "6"}, {"0", "1", "6"}}));

    const size_t interests = run("MATCH (c:Interest) RETURN c").size();
    const Rows crossed = run("MATCH (a {name:'Remy'}), (b {name:'Adam'}), (c:Interest) WITH a, b, c MATCH (a)-[e*1..3]->(b) RETURN a, b, c");
    EXPECT_GT(interests, 0u);
    EXPECT_EQ(crossed.size(), 2 * interests);
    for (const StringRowSink::Row& row : crossed) {
        EXPECT_EQ(row[0], "0");
        EXPECT_EQ(row[1], "1");
    }
}
