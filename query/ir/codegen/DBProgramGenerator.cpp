#include "DBProgramGenerator.h"

#include "CypherAST.h"

#include "VariableDependencyGraph.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Location.h"
#include "mlir/IR/MLIRContext.h"

#include "DBOps.h"

using namespace db;

void DBProgramGenerator::generate(const CypherAST* ast) {
    // MLIR setup
    mlir::MLIRContext ctxt;
    ctxt.loadDialect<mlir::db::DB>();
    mlir::OpBuilder builder(&ctxt);
    mlir::Location loc = builder.getUnknownLoc();
    auto mainMod = mlir::ModuleOp::create(loc);

    VariableDependencyGraph vdg;
    vdg.buildFromAST(ast);
}
