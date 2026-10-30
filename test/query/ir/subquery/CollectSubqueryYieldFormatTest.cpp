#include <gtest/gtest.h>

#include <string>

#include "llvm/Support/raw_ostream.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "NLDialect.h"
#include "StorageDialect.h"

namespace {

// COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name }: the body carries the row tag
constexpr const char* taggedYield = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %names = db.collect_subquery(%p) carries_scope {
  ^bb0(%pIn: !db.column<!storage.node_id>, %tagIn: !db.column<ui64>):
    %s, %e, %et, %i, %tag = db.get_out_edges_by_type(%pIn, ["INTERESTED_IN"], {%tagIn}) : (!db.column<!storage.node_id>, !db.column<ui64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<ui64>)
    %name = db.get_node_properties(%i, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    db.collect_subquery_yield tag %tag, %name, {} : !db.column<!storage.nullable<!storage.string>>
  } : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%names) : !db.column<none>
  return
}
)mlir";

// COLLECT { UNWIND [2, 1] AS k RETURN k ORDER BY k }: a body run once per row takes no tag,
// and names the columns it holds
constexpr const char* untaggedYield = R"mlir(
func.func @main() {
  %p = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %ks = db.collect_subquery(%p) {
  ^bb0(%pIn: !db.column<!storage.node_id>):
    %k = db.unwind_const([2, 1]) : !db.column<i64>
    %sorted = db.sort(%k) keys [0] ascending [true] : (!db.column<i64>) -> !db.column<i64>
    db.collect_subquery_yield %sorted, {%sorted} : !db.column<i64>, {!db.column<i64>}
  } : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%ks) : !db.column<none>
  return
}
)mlir";

// The textual form of db.collect_subquery_yield, whose value follows an optional row tag
class CollectSubqueryYieldFormatTest : public ::testing::Test {
protected:
    CollectSubqueryYieldFormatTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    // Printing the module and parsing the text back must give a module that verifies
    void expectRoundTrips(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = parse(programText);
        ASSERT_TRUE(module);

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module.get().print(stream);

        EXPECT_TRUE(parse(printed.c_str())) << printed;
    }

    mlir::MLIRContext _context;
};

TEST_F(CollectSubqueryYieldFormatTest, roundTripsATaggedYield) {
    expectRoundTrips(taggedYield);
}

TEST_F(CollectSubqueryYieldFormatTest, roundTripsAnUntaggedYield) {
    expectRoundTrips(untaggedYield);
}

}
