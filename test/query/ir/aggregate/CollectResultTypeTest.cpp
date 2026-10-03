#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "StorageDialect.h"

namespace {

// MATCH (a) WITH a.team AS team, collect(a.name) AS names RETURN team, names, with the
// element type of the trailing list result spelled by the caller. The frontend leaves a
// property column unresolved as !db.column<none>, so none is what this collect gathers
// and the only list element the lowering can produce for it.
std::string collectNamesProgram(const char* listElement) {
    const std::string listColumn = std::string("!db.column<!storage.list<") + listElement + ">>";

    return "func.func @main() {\n"
           "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
           "  %team = db.get_node_properties(%a, \"team\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           "  %name = db.get_node_properties(%a, \"name\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           "  %gteam, %names = db.collect(%team, %name) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, "
           + listColumn
           + ")\n"
             "  db.output(%gteam, %names) : !db.column<none>, "
           + listColumn
           + "\n"
             "  return\n"
             "}\n";
}

// MATCH (a) WITH collect(a) AS ids RETURN ids: the ungrouped (keys 0) collect over the
// scan's own node ID column, with the element type of the list result spelled by the
// caller. The value column is fully typed here, so the element gathered is
// !storage.node_id - the type check is a real comparison, not a none-against-none one.
std::string collectNodesProgram(const char* listElement) {
    const std::string listColumn = std::string("!db.column<!storage.list<") + listElement + ">>";

    return "func.func @main() {\n"
           "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
           "  %ids = db.collect(%a) keys 0 : (!db.column<!storage.node_id>) -> "
           + listColumn
           + "\n"
             "  db.output(%ids) : "
           + listColumn
           + "\n"
             "  return\n"
             "}\n";
}

// MATCH (a) WITH a.team AS team, collect(a.name) AS names UNWIND names AS name RETURN
// team, name, with the element type of the trailing value result spelled by the caller.
// The unwound value is one element of the collected column, so its type is the collected
// column's - none for an unresolved property column.
std::string unwindNamesProgram(const char* valueElement) {
    const std::string valueColumn = std::string("!db.column<") + valueElement + ">";

    return "func.func @main() {\n"
           "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
           "  %team = db.get_node_properties(%a, \"team\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           "  %name = db.get_node_properties(%a, \"name\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           "  %gteam, %uname = db.unwind_collect(%team, %name) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, "
           + valueColumn
           + ")\n"
             "  db.output(%gteam, %uname) : !db.column<none>, "
           + valueColumn
           + "\n"
             "  return\n"
             "}\n";
}

// MATCH (a) WITH collect(a) AS ids UNWIND ids AS id RETURN id: the ungrouped fused form
// over the scan's node ID column, with the element type of the value result spelled by
// the caller.
std::string unwindNodesProgram(const char* valueElement) {
    const std::string valueColumn = std::string("!db.column<") + valueElement + ">";

    return "func.func @main() {\n"
           "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
           "  %uid = db.unwind_collect(%a) keys 0 : (!db.column<!storage.node_id>) -> "
           + valueColumn
           + "\n"
             "  db.output(%uid) : "
           + valueColumn
           + "\n"
             "  return\n"
             "}\n";
}

}

// A context with the dialects a db-dialect collect program names: db for the ops,
// storage for the node ID and list types, func for the enclosing function.
class CollectResultTypeTest : public ::testing::Test {
protected:
    CollectResultTypeTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses a db-dialect module, returning a null module on failure. The verifier runs
    // as part of parsing, so an op the verifier rejects makes this null. The diagnostics
    // are swallowed so a deliberately mistyped program does not print to the test log.
    mlir::OwningOpRef<mlir::ModuleOp> parse(const std::string& programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    mlir::MLIRContext _context;
};

// The control for the two db.collect rejections below: a list result whose element is
// the collected column's own type is the shape the lowering produces, and it verifies.
TEST_F(CollectResultTypeTest, acceptsCollectListElementMatchingValueColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectNamesProgram("none"));
    EXPECT_TRUE(module);
}

// REVIEW.md #15: Collect::verify checks only that the trailing result is *some* list
// column, never that its element is the collected column's type. A list<i64> declared
// over a none value column passes the isa<storage::ListType> check, and lowerCollect
// then builds the emit chunk from the collected column instead, so the declared element
// type is silently discarded rather than diagnosed.
TEST_F(CollectResultTypeTest, verifierRejectsCollectListElementTypeMismatch) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectNamesProgram("i64"));
    EXPECT_FALSE(module);
}

// The control for the typed collect rejection: collecting the node ID column into a
// list of node IDs verifies.
TEST_F(CollectResultTypeTest, acceptsCollectNodeIDListElement) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectNodesProgram("!storage.node_id"));
    EXPECT_TRUE(module);
}

// The same hole on a fully typed value column: the collected column is node IDs, the
// declared list element is i64, and nothing compares the two.
TEST_F(CollectResultTypeTest, verifierRejectsCollectNodeIDsDeclaredAsInt64List) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectNodesProgram("i64"));
    EXPECT_FALSE(module);
}

// The control for the two db.unwind_collect rejections below: a value result carrying
// the collected column's type is the shape the lowering produces, and it verifies.
TEST_F(CollectResultTypeTest, acceptsUnwindValueResultMatchingValueColumn) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindNamesProgram("none"));
    EXPECT_TRUE(module);
}

// REVIEW.md #15: UnwindCollect::verify checks the operand and result counts and the
// pass-through of the keyCount grouping keys, but never compares results[keyCount] with
// columns[keyCount] - so a value result type that has nothing to do with the collected
// string column verifies. lowerUnwindCollect then builds the emit chunk from the
// collected column's type and buildLoopForSource rebinds the db result to that loop
// variable, so the declared type is discarded instead of diagnosed.
TEST_F(CollectResultTypeTest, verifierRejectsUnwindValueResultTypeMismatch) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindNamesProgram("!storage.node_id"));
    EXPECT_FALSE(module);
}

// The control for the typed unwind rejection: unwinding a collected node ID column back
// into node IDs verifies.
TEST_F(CollectResultTypeTest, acceptsUnwindNodeIDValueResult) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindNodesProgram("!storage.node_id"));
    EXPECT_TRUE(module);
}

// The same hole on a fully typed value column: the collected column is node IDs and the
// unwound value is declared i64.
TEST_F(CollectResultTypeTest, verifierRejectsUnwindNodeIDsDeclaredAsInt64) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unwindNodesProgram("i64"));
    EXPECT_FALSE(module);
}
