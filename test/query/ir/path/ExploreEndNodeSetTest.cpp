#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "DBOps.h"
#include "IRTestRows.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOps.h"
#include "StorageDialect.h"

#include "Graph.h"
#include "IRException.h"
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
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

// MATCH (a:Person {name:'Remy'})-[e]->+(b:Person {name:'Adam'}) RETURN a, e, b as codegen
// leaves it: the walk ends wherever it can and the name of each end is compared afterwards
const char* const endFilterProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_property_value("name", "Remy" : !storage.string, ["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %p = db.get_node_properties(%0#1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %c = db.constant("Adam" : !storage.string)
  %m = db.eq %p, %c : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%m, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%1#2, %1#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%1#0, %e, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same query with the ends resolved before the walk: the scan of the target runs first,
// its nodes are the set the walk heads for, and no filter follows it
const char* const endSetProgram = R"mlir(
func.func @main() {
  %b = db.scan_nodes_by_property_value("name", "Adam" : !storage.string, ["Person"]) : !db.column<!storage.node_id>
  %a = db.scan_nodes_by_property_value("name", "Remy" : !storage.string, ["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 end_nodes %b : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// Every path of two to three hops between two Persons, the end constrained by its label
const char* const endLabelProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 2 to 3 end_labels ["Person"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same, the label spelled as the set of the nodes carrying it
const char* const everyPersonSetProgram = R"mlir(
func.func @main() {
  %b = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 2 to 3 end_nodes %b : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// The pairs a walk of one to three hops between Persons reaches, and the same walk asked for
// each pair once: the distinct mode searches levels rather than walking trails, so the set is
// read there too
const char* const pairsToASetProgram = R"mlir(
func.func @main() {
  %b = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 to 3 end_nodes %b : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const distinctPairsToASetProgram = R"mlir(
func.func @main() {
  %b = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 to 3 end_nodes %b distinct : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// A set whose only listed node is not in the graph, so the walk can end nowhere
const char* const absentEndSetProgram = R"mlir(
func.func @main() {
  %b = db.const_scan_nodes([9999]) : !db.column<!storage.node_id>
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 0 to 3 end_nodes %b : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The ends of a walk cannot be bound twice
const char* const setBesideBoundEndProgram = R"mlir(
func.func @main() {
  %b = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#0, {%h#3}) forward hops 1 end_nodes %b end_column 0 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The set is scanned after the seeds, so its loop would only run once the walk is over
const char* const lateEndSetProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_property_value("name", "Remy" : !storage.string, ["Person"]) : !db.column<!storage.node_id>
  %b = db.scan_nodes_by_property_value("name", "Adam" : !storage.string, ["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 end_nodes %b : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Every trail of one to three hops over the generated graph, the seed, the end and the
// length of the path: the unconstrained rows a bound set is read against
const char* const generatedWalkProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

const char* const generatedEndSetProgram = R"mlir(
func.func @main() {
  %b = db.const_scan_nodes([11, 222, 333]) : !db.column<!storage.node_id>
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 to 3 end_nodes %b : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// Two hops over a graph whose balls are a small share of its nodes, which is where the
// target index is laid out as a table of the nodes it reached rather than a word per node
const char* const sparseWalkProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 to 2 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

const char* const sparseEndSetProgram = R"mlir(
func.func @main() {
  %b = db.const_scan_nodes([4242]) : !db.column<!storage.node_id>
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%a, {}) forward hops 1 to 2 end_nodes %b : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

}

// The end nodes of a whole exploration given as one set: the rows are read against the same
// walk constrained the long way, and against the walk with no constraint at all
class ExploreEndNodeSetTest : public ::testing::Test {
protected:
    ExploreEndNodeSetTest() {
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

    void expectSameRows(const char* constrainedProgram, const char* setProgram, const GraphView& view) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink constrained;
            runProgram(constrainedProgram, view, constrained, chunkSize);

            RowSink set;
            runProgram(setProgram, view, set, chunkSize);

            Rows expected;
            constrained.sortedRows(expected);

            Rows actual;
            set.sortedRows(actual);

            EXPECT_FALSE(expected.empty()) << "chunk size " << chunkSize;
            EXPECT_EQ(actual, expected) << "chunk size " << chunkSize;
        }
    }

    mlir::MLIRContext _context;
};

class ExploreEndNodeSetSimpleGraphTest : public ExploreEndNodeSetTest {
protected:
    ExploreEndNodeSetSimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

// Pseudo-random out-edges over a graph the test sizes, so a walk over every seed gives the
// executor the enumeration its target index has to beat
class ExploreEndNodeSetGeneratedGraphTest : public ExploreEndNodeSetTest {
protected:
    ExploreEndNodeSetGeneratedGraphTest()
        : _graph(Graph::create())
    {
        _jobSystem.init();
    }

    ~ExploreEndNodeSetGeneratedGraphTest() override {
        _jobSystem.terminate();
    }

    void generate(size_t nodeCount, size_t outDegree) {
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

    // The rows of @param walkProgram whose end is one of @param endNodes, which is what
    // @param setProgram must emit and nothing besides
    void expectTrailsLandingOn(const char* walkProgram,
                               const char* setProgram,
                               const std::set<std::string>& endNodes) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        RowSink walked;
        runProgram(walkProgram, view, walked);

        Rows expected;
        for (const Row& row : walked.rows()) {
            if (endNodes.contains(row[1])) {
                expected.push_back(row);
            }
        }
        std::sort(expected.begin(), expected.end());
        ASSERT_FALSE(expected.empty());

        RowSink set;
        runProgram(setProgram, view, set);

        Rows actual;
        set.sortedRows(actual);

        EXPECT_EQ(actual, expected);
    }

    JobSystem _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(ExploreEndNodeSetSimpleGraphTest, endsOnTheNodeTheSetHolds) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    expectSameRows(endFilterProgram, endSetProgram, view);
}

TEST_F(ExploreEndNodeSetSimpleGraphTest, endsOnEveryNodeOfALabelGivenAsASet) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    expectSameRows(endLabelProgram, everyPersonSetProgram, view);
}

TEST_F(ExploreEndNodeSetSimpleGraphTest, searchesTheSetForEachPairOnce) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    RowSink walked;
    runProgram(pairsToASetProgram, view, walked);

    Rows expected;
    walked.sortedRows(expected);
    expected.erase(std::unique(expected.begin(), expected.end()), expected.end());
    ASSERT_FALSE(expected.empty());

    RowSink searched;
    runProgram(distinctPairsToASetProgram, view, searched);

    Rows actual;
    searched.sortedRows(actual);

    EXPECT_EQ(actual, expected);
}

TEST_F(ExploreEndNodeSetSimpleGraphTest, emitsNoRowWhenTheSetIsEmpty) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    RowSink sink;
    runProgram(absentEndSetProgram, view, sink);

    EXPECT_TRUE(sink.rows().empty());
}

TEST_F(ExploreEndNodeSetSimpleGraphTest, rejectsASetBesideABoundEndColumn) {
    EXPECT_FALSE(parse(setBesideBoundEndProgram));
}

TEST_F(ExploreEndNodeSetSimpleGraphTest, fillsTheSetInALoopOfItsOwn) {
    const mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(endSetProgram);
    ASSERT_TRUE(dbModule);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    const mlir::OwningOpRef<mlir::ModuleOp> nlModule = lower(*dbModule, view);
    ASSERT_TRUE(mlir::succeeded(mlir::verify(*nlModule)));

    size_t buffers = 0;
    size_t collects = 0;
    (*nlModule)->walk([&buffers](mlir::nl::NodeSetBuffer) {
        buffers++;
    });
    (*nlModule)->walk([&collects](mlir::nl::NodeSetCollect) {
        collects++;
    });
    EXPECT_EQ(buffers, 1u);
    EXPECT_EQ(collects, 1u);

    mlir::nl::ExplorePaths exploration;
    (*nlModule)->walk([&exploration](mlir::nl::ExplorePaths op) {
        exploration = op;
    });
    ASSERT_TRUE(exploration);
    EXPECT_TRUE(exploration.getEndNodes());

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    (*nlModule)->print(stream);

    EXPECT_LT(printed.find("nl.node_set_collect"), printed.find("nl.explore_paths")) << printed;
}

TEST_F(ExploreEndNodeSetSimpleGraphTest, rejectsASetTheWalkWouldOutrun) {
    const mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(lateEndSetProgram);
    ASSERT_TRUE(dbModule);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    EXPECT_THROW(lower(*dbModule, view), IRException);
}

TEST_F(ExploreEndNodeSetGeneratedGraphTest, keepsTheTrailsLandingOnTheSet) {
    generate(600, 5);

    expectTrailsLandingOn(generatedWalkProgram, generatedEndSetProgram, {"11", "222", "333"});
}

TEST_F(ExploreEndNodeSetGeneratedGraphTest, keepsTheTrailsLandingOnASparselyReachedEnd) {
    generate(20000, 2);

    expectTrailsLandingOn(sparseWalkProgram, sparseEndSetProgram, {"4242"});
}
