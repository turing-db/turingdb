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

// MATCH (a) WITH collect(a.name) AS names, count(a.age) AS n RETURN names, n, with the
// reduction the accumulator carries spelled as its raw integer. The op is written in
// generic form because custom<GroupAggregateKinds> parses the symbolic keywords and
// rejects anything else, so the raw attribute is the only way an out-of-range kind
// reaches the verifier - which is how the C++ builder reaches it.
std::string collectAndReduceProgram(int64_t kind) {
    return "func.func @main() {\n"
           "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
           "  %name = db.get_node_properties(%a, \"name\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           "  %age = db.get_node_properties(%a, \"age\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           "  %names, %n = \"db.collect\"(%name, %age) {keyCount = 0 : ui64, kinds = array<i64: "
           + std::to_string(kind)
           + ">} : (!db.column<none>, !db.column<none>) -> (!db.column<!storage.list<none>>, !db.column<none>)\n"
             "  db.output(%names, %n) : !db.column<!storage.list<none>>, !db.column<none>\n"
             "  return\n"
             "}\n";
}

// count, the first kind of GroupAggregateKind
constexpr int64_t countKind = 0;

// One past count_rows, the last kind
constexpr int64_t unknownKind = 9;

}

// db.collect carries the reductions taken over its groups as a kinds array, exactly as
// db.group_aggregate does, so the same range check has to hold: a kind naming no
// reduction leaves the lowering nothing to build the fold from.
class CollectAggregateKindTest : public ::testing::Test {
protected:
    CollectAggregateKindTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses a db-dialect module, returning a null module on failure. The verifier runs
    // as part of parsing, so an op the verifier rejects makes this null. The diagnostics
    // are swallowed so a deliberately malformed program does not print to the test log.
    mlir::OwningOpRef<mlir::ModuleOp> parse(const std::string& programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    mlir::MLIRContext _context;
};

TEST_F(CollectAggregateKindTest, acceptsAKindNamingAReduction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectAndReduceProgram(countKind));
    EXPECT_TRUE(module);
}

TEST_F(CollectAggregateKindTest, rejectsAKindNamingNoReduction) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectAndReduceProgram(unknownKind));
    EXPECT_FALSE(module);
}

TEST_F(CollectAggregateKindTest, rejectsANegativeKind) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(collectAndReduceProgram(-1));
    EXPECT_FALSE(module);
}
