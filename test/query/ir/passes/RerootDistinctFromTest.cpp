#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "StorageDialect.h"

#include "IRTestOps.h"

using namespace turing::test;

namespace {

// VECTOR SEARCH ... YIELD ids MATCH (a)-[e1]->(n)-[e2]->(m) WHERE n = ids RETURN a, n, m,
// with the second hop excluding e1 through {}
constexpr std::string_view twoHopsSeededInTheMiddle = R"mlir(
func.func @main() {
  %0:4 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %s1, %e1, %et1, %n = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %s2, %e2, %et2, %m, %s1c, %e1c = db.get_out_edges(%n, {%s1, %e1}) {} : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>)
    db.yield %s1c, %s2, %m : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
    db.yield %ids : !db.column<!storage.node_id>
  }
  %1 = db.eq %0#1, %0#3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %2:3 = db.filter(%1, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%2#0, %2#1, %2#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

size_t countExcludingOps(mlir::ModuleOp module) {
    size_t count = 0;
    module.walk([&count](mlir::Operation* op) {
        if (op->hasAttr("distinct_from")) {
            count++;
        }
    });

    return count;
}

}

class RerootDistinctFromTest : public ::testing::Test {
protected:
    RerootDistinctFromTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parseWithExclusion(std::string_view exclusion) {
        std::string program(twoHopsSeededInTheMiddle);
        program.replace(program.find("{} :"), 2, exclusion);

        return mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));
    }

    bool runReroot(mlir::ModuleOp module) {
        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createRerootPatternAtSeed());

        return mlir::succeeded(passManager.run(module));
    }

    mlir::MLIRContext _context;
};

TEST_F(RerootDistinctFromTest, rerootsThePatternWithoutAnExclusion) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseWithExclusion("");
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReroot(*module));

    EXPECT_EQ(countOps<mlir::db::CrossProduct>(*module), 0u);
    EXPECT_EQ(countOps<mlir::db::ScanNodes>(*module), 0u);
}

TEST_F(RerootDistinctFromTest, keepsTheHopExcludingAnEarlierEdge) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parseWithExclusion("distinct_from [1]");
    ASSERT_TRUE(module);
    ASSERT_TRUE(runReroot(*module));

    EXPECT_EQ(countExcludingOps(*module), 1u);
}
