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
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
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

// MATCH (n:Person)-[e]->+(m:Person) RETURN n, e, m, size(e) as codegen leaves it: the end
// label is a filter after the exploration
const char* const filterFormProgram = R"mlir(
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

// The same query once the filter is folded into the exploration
const char* const fusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 end_labels ["Person"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %e, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// An edge type, a hop region and a carried column beside the end filter, all of which the
// fusion keeps; only the end node and the carried column are read afterwards
const char* const carriedFilterFormProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %h:4 = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %0:4 = db.explore_paths(%h#3, {%h#0}) forward hops 1 to 3 edge_types ["KNOWS_WELL"] {
  ^bb0(%src: !db.column<!storage.node_id>, %edge: !db.column<!storage.edge_id>, %end: !db.column<!storage.node_id>):
    %hls = db.get_node_label_set(%end) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
    %hok = db.check_label_constraint(%hls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
    db.yield %hok : !db.column<!storage.bool>
  } : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:4 = db.filter(%ok, {%0#0, %0#1, %0#2, %0#3}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>, !db.column<!storage.node_id>)
  db.output(%1#1, %1#3) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Two label filters over the same ends, which fold into one label list
const char* const chainedFiltersProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls2 = db.get_node_label_set(%1#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok2 = db.check_label_constraint(%ls2, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %2:3 = db.filter(%ok2, {%1#0, %1#1, %1#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%2#0, %2#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The label check reads the seeds, not the ends: a constraint the exploration cannot take
const char* const seedFilterProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#0) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%1#0, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The ends are also read outside the filter, so the rows the filter drops are still wanted
const char* const sharedEndsProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%1#0, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  db.output(%l) : !db.column<ui64>
  return
}
)mlir";

const char* const emptyEndLabelsProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 end_labels [] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

const char* const blankEndLabelProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 end_labels [""] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#1) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]->{0,0}(m:Person) RETURN n, m: the zero-length path of every Person alone
const char* const zeroLengthEndsProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 0 to 0 end_labels ["Person"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// An end label no node carries: not even the zero-length rows come out
const char* const unknownEndLabelProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 0 end_labels ["Nobody"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n:Person)-[e]-*(m:Interest) RETURN n, e, m, in both forms
const char* const bothToInterestFilterProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 0 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%1#2, %1#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%1#0, %e, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const bothToInterestFusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) both hops 0 end_labels ["Interest"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)<-[e]-*(m:Person) RETURN n, e, m from every node, in both forms
const char* const backwardToPersonFilterProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) backward hops 0 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%1#2, %1#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%1#0, %e, %1#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const backwardToPersonFusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) backward hops 0 end_labels ["Person"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %e = db.expand_path(%0#2, %0#0) kind edges : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  db.output(%0#0, %e, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.edge_id>>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (n)-[e]->{1,4}(m:T) RETURN n, m, size(e) over the generated graph, in both forms:
// enough seeds and fan-out for the executor to build the distance index
const char* const generatedFilterProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 4 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["T"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%1#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%1#0, %1#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

const char* const generatedFusedProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 4 end_labels ["T"] : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %l = db.path_length(%0#2) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  db.output(%0#0, %0#1, %l) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<ui64>
  return
}
)mlir";

// MATCH (n)-[e]->{2,4}(m:T) RETURN DISTINCT n, m over the generated graph: past a minimum of
// one hop the distinct form walks, and takes the pruning index
const char* const generatedWalkedFilterProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 2 to 4 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %ls = db.get_node_label_set(%0#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %ok = db.check_label_constraint(%ls, ["T"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %1:3 = db.filter(%ok, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%1#0, %1#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

const char* const generatedWalkedDistinctProgram = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 2 to 4 end_labels ["T"] distinct : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %d:2 = db.remove_duplicates(%0#0, %0#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

class ExploreEndConstraintTest : public ::testing::Test {
protected:
    ExploreEndConstraintTest() {
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

    static void expectEndLabels(mlir::db::ExplorePaths exploration, const std::vector<std::string>& expected) {
        const std::optional<mlir::ArrayAttr> endLabels = exploration.getEndLabels();
        ASSERT_TRUE(endLabels.has_value());

        std::vector<std::string> labels;
        for (const mlir::Attribute label : *endLabels) {
            labels.push_back(mlir::cast<mlir::StringAttr>(label).getValue().str());
        }
        EXPECT_EQ(labels, expected);
    }

    static void expectEdgeTypes(mlir::db::ExplorePaths exploration, const std::vector<std::string>& expected) {
        const std::optional<mlir::ArrayAttr> edgeTypes = exploration.getEdgeTypes();
        ASSERT_TRUE(edgeTypes.has_value());

        std::vector<std::string> names;
        for (const mlir::Attribute name : *edgeTypes) {
            names.push_back(mlir::cast<mlir::StringAttr>(name).getValue().str());
        }
        EXPECT_EQ(names, expected);
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

    // The fused form must emit the rows the filter form does, chunk size by chunk size
    void expectSameRows(const char* filterProgram, const char* fusedProgramText, const GraphView& view) {
        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink filtered;
            runProgram(filterProgram, view, filtered, chunkSize);

            RowSink fused;
            runProgram(fusedProgramText, view, fused, chunkSize);

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

class ExploreEndConstraintSimpleGraphTest : public ExploreEndConstraintTest {
protected:
    ExploreEndConstraintSimpleGraphTest()
        : _graph(Graph::create())
    {
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

// A generated graph large enough for the executor's cost gate to build the index: every
// node has three out-edges to pseudo-random nodes, one node in twenty carries T
class ExploreEndConstraintGeneratedGraphTest : public ExploreEndConstraintTest {
protected:
    static constexpr size_t nodeCount = 600;
    static constexpr size_t outDegree = 3;
    static constexpr size_t endEvery = 20;

    ExploreEndConstraintGeneratedGraphTest()
        : _graph(Graph::create())
    {
        _jobSystem.init();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelSet plain = LabelSet::fromList({metadata.getOrCreateLabel("N")});
        const LabelSet end = LabelSet::fromList({metadata.getOrCreateLabel("T")});
        const EdgeTypeID type = metadata.getOrCreateEdgeType("A");
        _endLabels = end;

        std::vector<NodeID> nodes;
        for (size_t node = 0; node < nodeCount; node++) {
            nodes.push_back(builder.addNode(node % endEvery == 0 ? end : plain));
        }

        uint64_t state = 12345;
        for (const NodeID source : nodes) {
            for (size_t edge = 0; edge < outDegree; edge++) {
                state = state * 6364136223846793005ull + 1442695040888963407ull;
                builder.addEdge(type, source, nodes[(state >> 33) % nodeCount]);
            }
        }

        const auto submitted = change->access().submit(_jobSystem);
        EXPECT_TRUE(submitted);
    }

    ~ExploreEndConstraintGeneratedGraphTest() override {
        _jobSystem.terminate();
    }

    JobSystem _jobSystem;
    std::unique_ptr<Graph> _graph;
    LabelSet _endLabels;
};

}

TEST_F(ExploreEndConstraintTest, roundTripsTheEndLabelsThroughThePrinter) {
    mlir::OwningOpRef<mlir::ModuleOp> module = parse(fusedProgram);
    ASSERT_TRUE(module);
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);
    EXPECT_NE(printed.find("end_labels [\"Person\"]"), std::string::npos) << printed;

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed.c_str());
    ASSERT_TRUE(reparsed) << printed;

    expectEndLabels(findExplorePaths(*reparsed), {"Person"});
    EXPECT_FALSE(findExplorePaths(*parse(filterFormProgram)).getEndLabels().has_value());
}

TEST_F(ExploreEndConstraintTest, rejectsEmptyAndBlankEndLabels) {
    for (const char* program : {emptyEndLabelsProgram, blankEndLabelProgram}) {
        mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        EXPECT_FALSE(module) << program;
    }
}

TEST_F(ExploreEndConstraintTest, fusesTheEndLabelFilterIntoTheExploration) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterFormProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseExploreEndConstraint());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    expectEndLabels(fused, {"Person"});

    // The filter and the label chain are gone, and the expansion reads the exploration
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::GetNodeLabelSet>(*module), 0u);

    mlir::db::ExpandPath expansion;
    (*module).walk([&](mlir::db::ExpandPath op) {
        expansion = op;
    });
    ASSERT_TRUE(expansion);
    EXPECT_EQ(expansion.getPaths(), fused.getPaths());
    EXPECT_EQ(expansion.getSrcids(), fused.getSrcids());
}

TEST_F(ExploreEndConstraintTest, keepsTheTypeTheRegionAndTheCarrySet) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(carriedFilterFormProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseExploreEndConstraint());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ExplorePaths fused = findExplorePaths(*module);
    ASSERT_TRUE(fused);
    expectEndLabels(fused, {"Person"});
    expectEdgeTypes(fused, {"KNOWS_WELL"});
    EXPECT_EQ(fused.getMaxHops(), std::optional<uint64_t> {3});
    EXPECT_EQ(fused.getColumnsToFilter().size(), 1u);
    ASSERT_FALSE(fused.getHop().empty());
    EXPECT_EQ(fused.getHop().front().getNumArguments(), 3u);

    // The hop region's own label check stays: it is a hop predicate, not an end filter
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::CheckLabelConstraint>(*module), 1u);

    // Trimming then drops the unread seed carry and keeps the end labels
    runPass(*module, mlir::db::createTrimUnreadColumns());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    mlir::db::ExplorePaths trimmed = findExplorePaths(*module);
    ASSERT_TRUE(trimmed);
    expectEndLabels(trimmed, {"Person"});
    EXPECT_EQ(trimmed.getColumnsToFilter().size(), 1u);
    expectEdgeTypes(trimmed, {"KNOWS_WELL"});
}

TEST_F(ExploreEndConstraintTest, mergesChainedLabelFilters) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(chainedFiltersProgram);
    ASSERT_TRUE(module);

    runPass(*module, mlir::db::createFuseExploreEndConstraint());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

    expectEndLabels(findExplorePaths(*module), {"Interest", "Person"});
    EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 0u);
}

TEST_F(ExploreEndConstraintTest, leavesFiltersItCannotTake) {
    for (const char* program : {seedFilterProgram, sharedEndsProgram}) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
        ASSERT_TRUE(module) << program;

        runPass(*module, mlir::db::createFuseExploreEndConstraint());
        EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));

        EXPECT_FALSE(findExplorePaths(*module).getEndLabels().has_value()) << program;
        EXPECT_EQ(countOps<mlir::db::FilterOp>(*module), 1u) << program;
    }
}

TEST_F(ExploreEndConstraintSimpleGraphTest, lowersTheEndLabelsOntoTheIterator) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const mlir::OwningOpRef<mlir::ModuleOp> dbModule = parse(fusedProgram);
    ASSERT_TRUE(dbModule);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = lower(*dbModule, reader.getView());
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*nlModule)));

    mlir::nl::ExplorePaths exploration;
    nlModule->walk([&](mlir::nl::ExplorePaths found) {
        exploration = found;
    });
    ASSERT_TRUE(exploration);

    const std::optional<mlir::ArrayAttr> endLabels = exploration.getEndLabels();
    ASSERT_TRUE(endLabels.has_value());
    ASSERT_EQ(endLabels->size(), 1u);
    EXPECT_EQ(mlir::cast<mlir::StringAttr>((*endLabels)[0]).getValue(), "Person");
}

TEST_F(ExploreEndConstraintSimpleGraphTest, fusedExplorationWalksPersonToPersonPathsAsTheOracle) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The filter form, fused by the pass, and the fused form spelled out both give the
    // first variable-length oracle
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

    for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
        RowSink spelledOut;
        runProgram(fusedProgram, reader.getView(), spelledOut, chunkSize);

        Rows rows;
        spelledOut.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;

        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(filterFormProgram);
        ASSERT_TRUE(module);
        runPass(*module, mlir::db::createFuseExploreEndConstraint());

        RowSink fusedByThePass;
        runModule(*module, reader.getView(), fusedByThePass, chunkSize);
        fusedByThePass.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "chunk size " << chunkSize;
    }
}

TEST_F(ExploreEndConstraintSimpleGraphTest, fusedFormsEmitTheFilteredRowsInEveryDirection) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    expectSameRows(bothToInterestFilterProgram, bothToInterestFusedProgram, view);
    expectSameRows(backwardToPersonFilterProgram, backwardToPersonFusedProgram, view);
}

TEST_F(ExploreEndConstraintSimpleGraphTest, zeroLengthRowsNeedTheSeedToCarryTheLabels) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    RowSink sink;
    runProgram(zeroLengthEndsProgram, reader.getView(), sink);

    // Eight Persons, each its own zero-length path; no Interest or Supernatural node
    const Rows& rows = sink.rows();
    EXPECT_EQ(rows.size(), 8u);
    for (const Row& row : rows) {
        ASSERT_EQ(row.size(), 2u);
        EXPECT_EQ(row[0], row[1]);
    }

    RowSink nobody;
    runProgram(unknownEndLabelProgram, reader.getView(), nobody);
    EXPECT_TRUE(nobody.rows().empty());
}

TEST_F(ExploreEndConstraintSimpleGraphTest, chainedLabelsNoNodeCarriesTogetherEmitNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(chainedFiltersProgram);
    ASSERT_TRUE(module);
    runPass(*module, mlir::db::createFuseExploreEndConstraint());

    RowSink sink;
    runModule(*module, reader.getView(), sink);
    EXPECT_TRUE(sink.rows().empty());
}

TEST_F(ExploreEndConstraintGeneratedGraphTest, pruningIndexKeepsTheFilteredRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    ASSERT_EQ(reader.getNodeCount(), nodeCount);

    // The scan hands the executor every node as a seed in one chunk, past the cost gate
    std::vector<NodeID> seeds;
    for (size_t node = 0; node < nodeCount; node++) {
        seeds.push_back(NodeID(node));
    }

    const PartDirectory parts(view);
    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, expansion);

    const double walkChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, nodeCount, 4);
    PathDistanceIndex index;
    EXPECT_TRUE(index.buildWithin(view, _endLabels, PathExplorationDir::FORWARD, {}, 4, walkChecks));

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

TEST_F(ExploreEndConstraintGeneratedGraphTest, distinctWalkWithThePruningIndexKeepsTheDeduplicatedRows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();
    ASSERT_EQ(reader.getNodeCount(), nodeCount);

    std::vector<NodeID> seeds;
    for (size_t node = 0; node < nodeCount; node++) {
        seeds.push_back(NodeID(node));
    }

    const PartDirectory parts(view);
    PathDistanceIndex::SeedExpansion expansion;
    PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds, expansion);

    const double walkChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, nodeCount, 4);
    PathDistanceIndex index;
    EXPECT_TRUE(index.buildWithin(view, _endLabels, PathExplorationDir::FORWARD, {}, 4, walkChecks));

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
