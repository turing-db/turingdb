#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

// Chains long enough that a pass recursing once per link overflows the stack
class DeepChainPassesTest : public ::testing::Test {
protected:
    static constexpr size_t CHAIN_LENGTH = 250000;

    DeepChainPassesTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const std::string& programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    bool runPass(mlir::ModuleOp module, std::unique_ptr<mlir::Pass> pass) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(std::move(pass));

        return mlir::succeeded(passManager.run(module));
    }

    mlir::MLIRContext _context;
};

TEST_F(DeepChainPassesTest, fusesAPropertyScanUnderALongConjunction) {
    std::string program = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%n, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(32 : i64)
  %m0 = db.eq %age, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
)mlir";

    for (size_t link = 1; link <= CHAIN_LENGTH; link++) {
        const std::string index = std::to_string(link);
        const std::string previous = std::to_string(link - 1);

        program += "  %c" + index + " = db.constant(" + index + " : i64)\n";
        program += "  %ne" + index + " = db.neq %age, %c" + index
                 + " : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>\n";
        program += "  %m" + index + " = db.and %m" + previous + ", %ne" + index
                 + " : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>\n";
    }

    program += "  %nf = db.filter(%m" + std::to_string(CHAIN_LENGTH)
             + ", {%n}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n";
    program += "  db.output(%nf) : !db.column<!storage.node_id>\n  return\n}\n";

    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPass(*module, mlir::db::createFuseScanByPropertyValue()));

    EXPECT_EQ(countOps<mlir::db::ScanNodesByPropertyValue>(*module), 1u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
}

TEST_F(DeepChainPassesTest, dedupsTheEndsOfAnExplorationFilteredByALongDisjunction) {
    std::string program = R"mlir(
func.func @main() {
  %n = db.scan_nodes() : !db.column<!storage.node_id>
  %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %age = db.get_node_properties(%0#1, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
)mlir";

    // The walk visits the users of the age newest first, so the equality the chain starts
    // from is written last.
    for (size_t offset = 0; offset <= CHAIN_LENGTH; offset++) {
        const std::string index = std::to_string(CHAIN_LENGTH - offset);

        program += "  %c" + index + " = db.constant(" + index + " : i64)\n";
        program += "  %e" + index + " = db.eq %age, %c" + index
                 + " : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>\n";
    }

    for (size_t link = 1; link <= CHAIN_LENGTH; link++) {
        const std::string index = std::to_string(link);
        const std::string previous = link == 1 ? "%e0" : "%m" + std::to_string(link - 1);

        program += "  %m" + index + " = db.or " + previous + ", %e" + index
                 + " : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>\n";
    }

    program += "  %1:2 = db.filter(%m" + std::to_string(CHAIN_LENGTH)
             + ", {%0#0, %0#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)"
               " -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)\n";
    program += "  %d:2 = db.remove_duplicates(%1#0, %1#1) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)"
               " -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)\n";
    program += "  db.output(%d#0, %d#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>\n  return\n}\n";

    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPass(*module, mlir::db::createFuseExploreDistinctEnds()));

    llvm::SmallVector<mlir::db::ExplorePaths> explorations = collect<mlir::db::ExplorePaths>(*module);
    ASSERT_EQ(explorations.size(), 1u);
    EXPECT_TRUE(explorations.front().getDistinct());
}

TEST_F(DeepChainPassesTest, reusesAPropertyReadFromAboveALongChainOfFilters) {
    std::string program = R"mlir(
func.func @main() {
  %f0 = db.scan_nodes() : !db.column<!storage.node_id>
  %age = db.get_node_properties(%f0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %true = db.constant(true)
)mlir";

    for (size_t link = 1; link <= CHAIN_LENGTH; link++) {
        program += "  %f" + std::to_string(link) + " = db.filter(%true, {%f" + std::to_string(link - 1)
                 + "}) : (!db.column<i1>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n";
    }

    const std::string last = std::to_string(CHAIN_LENGTH);
    program += "  %again = db.get_node_properties(%f" + last
             + ", \"age\") : (!db.column<!storage.node_id>) -> !db.column<none>\n";
    program += "  db.output(%age, %again) : !db.column<none>, !db.column<none>\n  return\n}\n";

    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(program);
    ASSERT_TRUE(module);
    ASSERT_TRUE(runPass(*module, mlir::db::createReusePropertyReads()));

    EXPECT_EQ(countOps<mlir::db::GetNodeProperties>(*module), 1u);
}
