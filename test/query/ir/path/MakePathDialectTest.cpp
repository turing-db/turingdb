#include <gtest/gtest.h>

#include <string>
#include <string_view>

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

namespace {

// db.make_path and nl.make_path share one verifier, and the executor reads its operands as
// the steps that verifier admits: a node opens the path, an edge lands on a node, a
// forward walk continues it, and a reversed walk closes on the node it seeded from.
class MakePathDialectTest : public ::testing::Test {
protected:
    MakePathDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    bool parsesInDB(std::string_view makePath) {
        const std::string program = std::string(R"mlir(
!node = !db.column<!storage.node_id>
!edge = !db.column<!storage.edge_id>
!walk = !db.column<!storage.path_ref>
!path = !db.column<!storage.path>
func.func @main(%a: !node, %b: !node, %k: !node, %x: !edge, %pe: !walk) {
  %p = db.make_path)mlir") + std::string(makePath) + R"mlir(
  return
}
)mlir";

        return parses(program);
    }

    bool parsesInNL(std::string_view makePath) {
        const std::string program = std::string(R"mlir(
!node = !nl.chunk<!storage.node_id>
!edge = !nl.chunk<!storage.edge_id>
!walk = !nl.chunk<!storage.path_ref>
!path = !nl.chunk<!storage.path>
func.func @main(%a: !node, %b: !node, %k: !node, %x: !edge, %pe: !walk) {
  %p = nl.make_path)mlir") + std::string(makePath) + R"mlir(
  return
}
)mlir";

        return parses(program);
    }

    bool parses(const std::string& program) {
        return static_cast<bool>(mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context)));
    }

    mlir::MLIRContext _context;
};

constexpr std::string_view OPENS_ON_A_NODE_AND_HOPS = "(%a, %x, %b) : (!node, !edge, !node) -> !path";
constexpr std::string_view CONTINUES_WITH_A_WALK = "(%a, %x, %b, %pe) : (!node, !edge, !node, !walk) -> !path";
constexpr std::string_view OPENS_ON_A_REVERSED_WALK = "(%pe, %b) {reversed_paths = [0]} : (!walk, !node) -> !path";
constexpr std::string_view CLOSES_A_REVERSED_WALK_MID_PATH = "(%a, %x, %k, %pe, %b) {reversed_paths = [3]} : (!node, !edge, !node, !walk, !node) -> !path";

constexpr std::string_view OPENS_ON_AN_EDGE = "(%x, %b) : (!edge, !node) -> !path";
constexpr std::string_view OPENS_ON_A_FORWARD_WALK = "(%pe) : (!walk) -> !path";
constexpr std::string_view ENDS_ON_AN_EDGE = "(%a, %x) : (!node, !edge) -> !path";
constexpr std::string_view TAKES_TWO_NODES_IN_A_ROW = "(%a, %b) : (!node, !node) -> !path";
constexpr std::string_view LEAVES_A_REVERSED_WALK_OPEN = "(%a, %pe) {reversed_paths = [1]} : (!node, !walk) -> !path";

TEST_F(MakePathDialectTest, acceptsEveryStepInDB) {
    EXPECT_TRUE(parsesInDB(OPENS_ON_A_NODE_AND_HOPS));
    EXPECT_TRUE(parsesInDB(CONTINUES_WITH_A_WALK));
    EXPECT_TRUE(parsesInDB(OPENS_ON_A_REVERSED_WALK));
    EXPECT_TRUE(parsesInDB(CLOSES_A_REVERSED_WALK_MID_PATH));
}

TEST_F(MakePathDialectTest, acceptsEveryStepInNL) {
    EXPECT_TRUE(parsesInNL(OPENS_ON_A_NODE_AND_HOPS));
    EXPECT_TRUE(parsesInNL(CONTINUES_WITH_A_WALK));
    EXPECT_TRUE(parsesInNL(OPENS_ON_A_REVERSED_WALK));
    EXPECT_TRUE(parsesInNL(CLOSES_A_REVERSED_WALK_MID_PATH));
}

TEST_F(MakePathDialectTest, rejectsAPathWithoutOneNodeMoreThanEdgesInDB) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parsesInDB(OPENS_ON_AN_EDGE));
    EXPECT_FALSE(parsesInDB(OPENS_ON_A_FORWARD_WALK));
    EXPECT_FALSE(parsesInDB(ENDS_ON_AN_EDGE));
    EXPECT_FALSE(parsesInDB(TAKES_TWO_NODES_IN_A_ROW));
    EXPECT_FALSE(parsesInDB(LEAVES_A_REVERSED_WALK_OPEN));
}

TEST_F(MakePathDialectTest, rejectsAPathWithoutOneNodeMoreThanEdgesInNL) {
    const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
        return mlir::success();
    });

    EXPECT_FALSE(parsesInNL(OPENS_ON_AN_EDGE));
    EXPECT_FALSE(parsesInNL(OPENS_ON_A_FORWARD_WALK));
    EXPECT_FALSE(parsesInNL(ENDS_ON_AN_EDGE));
    EXPECT_FALSE(parsesInNL(TAKES_TWO_NODES_IN_A_ROW));
    EXPECT_FALSE(parsesInNL(LEAVES_A_REVERSED_WALK_OPEN));
}

}
