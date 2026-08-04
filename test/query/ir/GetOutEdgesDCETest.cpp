#include <gtest/gtest.h>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
#include "DBTypes.h"
#include "StorageDialect.h"
#include "StorageTypes.h"

namespace {

// A context with the dialects a db-dialect get_out_edges program needs: db for
// the query ops and func for the enclosing function.
class GetOutEdgesDCETest : public ::testing::Test {
protected:
    GetOutEdgesDCETest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    mlir::OwningOpRef<mlir::ModuleOp> parse(const char* programText) {
        return mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
    }

    // The single db.get_out_edges in the module's only function.
    static mlir::db::GetOutEdges findGetOutEdges(mlir::ModuleOp module) {
        mlir::db::GetOutEdges edges;
        module.walk([&](mlir::db::GetOutEdges op) {
            edges = op;
        });

        return edges;
    }

    mlir::MLIRContext _context;
};

// MATCH (a)-->(b) RETURN b: a scan feeds one out-edge hop, and only the target
// column (b) is returned. The source, edge and edge-type columns are dead - no op
// reads them - so the pass should retype exactly those three to !db.nullptr.
const char* const unusedEdgeColumnsProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %s, %e, %et, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%b) : !db.column<!storage.node_id>
  return
}
)mlir";

TEST_F(GetOutEdgesDCETest, retypesUnreadFixedColumnsToNullptr) {
    const mlir::OwningOpRef<mlir::ModuleOp> module = parse(unusedEdgeColumnsProgram);
    ASSERT_TRUE(module);

    mlir::PassManager passManager(&_context);
    passManager.addPass(mlir::db::createGetOutEdgesDCE());
    ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));

    llvm::outs() << "\n--- after get_out_edges_dce ---\n";
    module.get().print(llvm::outs());
    llvm::outs() << "\n";

    mlir::db::GetOutEdges edges = findGetOutEdges(*module);
    ASSERT_TRUE(edges);

    // The three unread columns are now !db.nullptr; the returned target column
    // keeps its node ID type.
    const mlir::Type nullType = mlir::db::NullptrType::get(&_context);
    const mlir::Type nodeIDColumnType = mlir::db::ColumnType::get(&_context, mlir::storage::NodeIDType::get(&_context));

    EXPECT_EQ(edges.getSrcids().getType(), nullType);
    EXPECT_EQ(edges.getEids().getType(), nullType);
    EXPECT_EQ(edges.getEtypes().getType(), nullType);
    EXPECT_EQ(edges.getTgtids().getType(), nodeIDColumnType);

    // The relaxed result constraints accept the null retyping, so the module
    // still verifies.
    EXPECT_TRUE(mlir::succeeded(mlir::verify(*module)));
}

}
