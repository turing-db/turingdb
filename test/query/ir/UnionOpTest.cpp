#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "StorageDialect.h"

namespace {

// One branch of a union: a scan, the name it reads and the db.output naming the column it
// fills the result table with. The label, the output and any dedup are the caller's, so
// two branches differ only where the test means them to.
std::string branch(const char* label, const char* output) {
    return "  {\n"
           "    %n = db.scan_nodes_by_label([\"" + std::string(label) + "\"]) : !db.column<!storage.node_id>\n"
           "    %name = db.get_node_properties(%n, \"name\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
           + std::string(output)
           + "\n  }";
}

std::string unionProgram(const std::string& first, const std::string& second, const char* preamble = "") {
    return "func.func @main() {\n"
           + std::string(preamble)
           + "  db.union\n" + first + ",\n" + second + "\n"
             "  return\n"
             "}\n";
}

const char* const outputsName = "    db.output(%name) names [\"name\"] : !db.column<none>";
const char* const outputsOther = "    db.output(%name) names [\"other\"] : !db.column<none>";
const char* const outputsTwoColumns
    = "    db.output(%name, %name) names [\"name\", \"again\"] : !db.column<none>, !db.column<none>";
const char* const outputsNothing = "    %unused = db.constant(1 : i64)";

// A deduping union: the set is opened above the op and both branches record their rows in
// it, which is what makes the dedup span them
const char* const opensADistinctSet = "  %seen = db.distinct_set : !db.distinct_set\n";
const char* const dedupsAgainstTheSet
    = "    %deduped = db.remove_duplicates(%name) against %seen : (!db.column<none>) -> !db.column<none>\n"
      "    db.output(%deduped) names [\"name\"] : !db.column<none>";

}

// db.union's branch regions and db.remove_duplicates' shared set print and parse back, and
// the verifier holds what makes a union one result table: every branch ends in a db.output,
// and they all name the same columns.
class UnionOpTest : public ::testing::Test {
protected:
    UnionOpTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses a db-dialect module, returning a null module on failure. The verifier runs as
    // part of parsing, so an op the verifier rejects makes this null. The diagnostics are
    // swallowed so a deliberately malformed program does not print to the test log.
    mlir::OwningOpRef<mlir::ModuleOp> parse(const std::string& programText) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    mlir::MLIRContext _context;
};

TEST_F(UnionOpTest, parsesTwoBranches) {
    mlir::OwningOpRef<mlir::ModuleOp> module =
        parse(unionProgram(branch("Founder", outputsName), branch("Interest", outputsName)));
    ASSERT_TRUE(module);

    mlir::db::Union unionOp;
    module->walk([&unionOp](mlir::db::Union op) { unionOp = op; });

    ASSERT_TRUE(unionOp);
    EXPECT_EQ(unionOp.getBranches().size(), 2U);
}

TEST_F(UnionOpTest, parsesThreeBranches) {
    const std::string program =
        "func.func @main() {\n"
        "  db.union\n" + branch("Founder", outputsName)
        + ",\n" + branch("Interest", outputsName)
        + ",\n" + branch("Person", outputsName)
        + "\n  return\n}\n";

    mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
    ASSERT_TRUE(module);

    mlir::db::Union unionOp;
    module->walk([&unionOp](mlir::db::Union op) { unionOp = op; });

    ASSERT_TRUE(unionOp);
    EXPECT_EQ(unionOp.getBranches().size(), 3U);
}

// The set is one value both branches read, so the dedup they run is one dedup
TEST_F(UnionOpTest, parsesASharedDistinctSet) {
    mlir::OwningOpRef<mlir::ModuleOp> module =
        parse(unionProgram(branch("Founder", dedupsAgainstTheSet),
                           branch("Interest", dedupsAgainstTheSet),
                           opensADistinctSet));
    ASSERT_TRUE(module);

    mlir::Value set;
    bool sharesOneSet = true;
    module->walk([&](mlir::db::RemoveDuplicates dedup) {
        if (!set) {
            set = dedup.getSet();
            return;
        }

        sharesOneSet = sharesOneSet && dedup.getSet() == set;
    });

    ASSERT_TRUE(set);
    EXPECT_TRUE(sharesOneSet);
}

// The set is the one optional operand of db.remove_duplicates, so a dedup keeping a set of
// its own - what a plain RETURN DISTINCT emits - parses in the same form without it
TEST_F(UnionOpTest, parsesADedupWithNoSharedSet) {
    const std::string program =
        "func.func @main() {\n"
        "  %n = db.scan_nodes() : !db.column<!storage.node_id>\n"
        "  %d = db.remove_duplicates(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
        "  db.output(%d) : !db.column<!storage.node_id>\n"
        "  return\n}\n";

    mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
    ASSERT_TRUE(module);

    mlir::db::RemoveDuplicates dedup;
    module->walk([&dedup](mlir::db::RemoveDuplicates op) { dedup = op; });

    ASSERT_TRUE(dedup);
    EXPECT_FALSE(dedup.getSet());
}

TEST_F(UnionOpTest, printsWhatItParses) {
    mlir::OwningOpRef<mlir::ModuleOp> module =
        parse(unionProgram(branch("Founder", dedupsAgainstTheSet),
                           branch("Interest", dedupsAgainstTheSet),
                           opensADistinctSet));
    ASSERT_TRUE(module);

    std::string printed;
    llvm::raw_string_ostream stream(printed);
    module->print(stream);

    const mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed);
    EXPECT_TRUE(reparsed) << printed;
}

TEST_F(UnionOpTest, verifierRejectsABranchThatOutputsNothing) {
    EXPECT_FALSE(parse(unionProgram(branch("Founder", outputsName), branch("Interest", outputsNothing))));
}

TEST_F(UnionOpTest, verifierRejectsBranchesNamingOtherColumns) {
    EXPECT_FALSE(parse(unionProgram(branch("Founder", outputsName), branch("Interest", outputsOther))));
}

TEST_F(UnionOpTest, verifierRejectsBranchesOfDifferentWidths) {
    EXPECT_FALSE(parse(unionProgram(branch("Founder", outputsName), branch("Interest", outputsTwoColumns))));
}

// A union of one branch is that branch; the op stands for a concatenation, so it needs two
TEST_F(UnionOpTest, verifierRejectsASingleBranch) {
    const std::string program =
        "func.func @main() {\n"
        "  db.union\n" + branch("Founder", outputsName) + "\n"
        "  return\n}\n";

    EXPECT_FALSE(parse(program));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
