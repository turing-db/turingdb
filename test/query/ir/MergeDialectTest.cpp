#include <gtest/gtest.h>

#include <string>

#include "llvm/Support/raw_ostream.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "NLDialect.h"
#include "NLOps.h"
#include "StorageDialect.h"
#include "StorageEnums.h"

namespace {

// The textual form of db.merge and nl.merge: what a MERGE pattern's chain looks like
// in each dialect, and the shape rules the two share.
class MergeDialectTest : public ::testing::Test {
protected:
    MergeDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    // The negative cases install their own swallowing handler, so a diagnostic reaching
    // here belongs to a program that was meant to parse and is worth printing
    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool parses(const char* programText) {
        return static_cast<bool>(parse(programText));
    }

    // Printing the module and parsing the text back must give a module that verifies:
    // the printer and the parser are inverses over the merge's attribute lists.
    void expectRoundTrips(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module.get().print(stream);

        EXPECT_TRUE(parses(printed.c_str())) << printed;
    }

    mlir::MLIRContext _context;
};

// One chain node, matched and written by its label and one property value
constexpr const char* mergesOneNode = R"mlir(
func.func @main() {
  %name = db.constant("Alice" : !storage.string)
  %n, %n_pending, %created = db.merge nodes [["Person"]] props [["name"]]
                                      edges [] props [] dirs []
                                      bound {} pending [] {} values {%name} {} carrying {}
    : (!db.column<!storage.string>)
      -> (!db.column<!storage.node_id>, !db.column<!storage.bool>, !db.column<!storage.bool>)
  db.output(%n) : !db.column<!storage.node_id>
  return
}
)mlir";

// A hop between one bound end and one the pattern describes, carrying the bound
// column past the merge. The bound end has no result pair: it comes back through the
// carry set.
constexpr const char* mergesAHopOntoABoundNode = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %tag = db.constant("x" : !storage.string)
  %b, %b_pending, %e, %e_pending, %created, %carried
    = db.merge nodes [[], ["Tag"]] props [[], ["name"]]
               edges ["TAGGED"] props [["weight"]] dirs [forward]
               bound {%a} pending [] {} values {%tag} {%tag} carrying {%a}
    : (!db.column<!storage.node_id>, !db.column<!storage.string>, !db.column<!storage.string>, !db.column<!storage.node_id>)
      -> (!db.column<!storage.node_id>, !db.column<!storage.bool>,
          !db.column<!storage.edge_id>, !db.column<!storage.bool>,
          !db.column<!storage.bool>, !db.column<!storage.node_id>)
  db.output(%b) : !db.column<!storage.node_id>
  return
}
)mlir";

// The nl sibling, driven inside the loop that binds its input chunk
constexpr const char* mergesOneNodeInALoop = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %node in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %n, %n_pending, %created, %carried = nl.merge nodes [["Tag"]] props [[]]
                                                  edges [] props [] dirs []
                                                  bound {} pending [] {} values {} {} carrying {%node}
      : (!nl.chunk<!storage.node_id>)
        -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.bool>,
            !nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>)
  }
  return
}
)mlir";

// A chain node with no label is one the query bound, so its column has to come in
// through `bound`
constexpr const char* mergesALabellessNodeWithNoBoundColumn = R"mlir(
func.func @main() {
  %created = db.merge nodes [[]] props [[]]
                      edges [] props [] dirs []
                      bound {} pending [] {} values {} {} carrying {}
    : () -> (!db.column<!storage.bool>)
  return
}
)mlir";

// Two chain nodes are joined by exactly one hop
constexpr const char* mergesTwoNodesWithNoHop = R"mlir(
func.func @main() {
  %n, %n_pending, %m, %m_pending, %e, %e_pending, %created
    = db.merge nodes [["A"], ["B"]] props [[], []]
               edges [] props [] dirs []
               bound {} pending [] {} values {} {} carrying {}
    : () -> (!db.column<!storage.node_id>, !db.column<!storage.bool>,
             !db.column<!storage.node_id>, !db.column<!storage.bool>,
             !db.column<!storage.edge_id>, !db.column<!storage.bool>,
             !db.column<!storage.bool>)
  return
}
)mlir";

// One property name per value column
constexpr const char* mergesANodeWithNoValueForItsProperty = R"mlir(
func.func @main() {
  %n, %n_pending, %created = db.merge nodes [["Person"]] props [["name"]]
                                      edges [] props [] dirs []
                                      bound {} pending [] {} values {} {} carrying {}
    : () -> (!db.column<!storage.node_id>, !db.column<!storage.bool>, !db.column<!storage.bool>)
  return
}
)mlir";

// The masks name their chain nodes in increasing order
constexpr const char* mergesTwoBoundNodesWithOutOfOrderPendingMasks = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %mask = db.check_label_constraint(%a, ["Person"]) : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %e, %e_pending, %created
    = db.merge nodes [[], []] props [[], []]
               edges ["KNOWS"] props [[]] dirs [forward]
               bound {%a, %a} pending [1, 0] {%mask, %mask} values {} {} carrying {}
    : (!db.column<!storage.node_id>, !db.column<!storage.node_id>,
       !db.column<!storage.bool>, !db.column<!storage.bool>)
      -> (!db.column<!storage.edge_id>, !db.column<!storage.bool>,
          !db.column<!storage.bool>)
  return
}
)mlir";

TEST_F(MergeDialectTest, parsesAOneNodePattern) {
    expectRoundTrips(mergesOneNode);
}

TEST_F(MergeDialectTest, parsesAHopOntoABoundNode) {
    expectRoundTrips(mergesAHopOntoABoundNode);
}

TEST_F(MergeDialectTest, parsesTheNLSiblingInALoop) {
    expectRoundTrips(mergesOneNodeInALoop);
}

// The direction list prints as keywords rather than as the integers it is stored as
TEST_F(MergeDialectTest, printsHopDirectionsAsKeywords) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(mergesAHopOntoABoundNode);
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module.get().print(stream);

    EXPECT_NE(printed.find("dirs [forward]"), std::string::npos) << printed;
}

TEST_F(MergeDialectTest, readsTheDirectionOfEachHop) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(mergesAHopOntoABoundNode);
    ASSERT_TRUE(module);

    mlir::db::Merge merge;
    module.get().walk([&](mlir::db::Merge found) {
        merge = found;
    });
    ASSERT_TRUE(merge);

    const llvm::ArrayRef<int64_t> directions = merge.getEdgeDirections();
    ASSERT_EQ(directions.size(), 1u);
    EXPECT_EQ(static_cast<mlir::storage::EdgeDirection>(directions[0]), mlir::storage::EdgeDirection::Forward);
}

TEST_F(MergeDialectTest, verifierRejectsALabellessNodeWithNoBoundColumn) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(mergesALabellessNodeWithNoBoundColumn));
}

TEST_F(MergeDialectTest, verifierRejectsTwoNodesWithNoHop) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(mergesTwoNodesWithNoHop));
}

TEST_F(MergeDialectTest, verifierRejectsAPropertyNameWithNoValue) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(mergesANodeWithNoValueForItsProperty));
}

TEST_F(MergeDialectTest, verifierRejectsOutOfOrderPendingMasks) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parses(mergesTwoBoundNodesWithOutOfOrderPendingMasks));
}

}
