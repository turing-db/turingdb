#include "DBProgramGenerator.h"

#include "CypherAST.h"

#include "DBTypes.h"
#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"
#include "VariableDependencyGraphTraversal.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Location.h"
#include "mlir/IR/MLIRContext.h"

#include "DBOps.h"

using namespace db;

mlir::db::ColumnType DBProgramGenerator::createColumnFor(const VariableDependency* var) {
    const auto& existingVars = _varMap[var];
    const std::string colName = std::string(var->getName()) + std::to_string(existingVars.size());
    const mlir::db::ColumnType col = mlir::db::ColumnType::get(&_mlirCtxt, colName);
    _varMap[var].emplace_back(col);

    return col;
}

void DBProgramGenerator::generate(const CypherAST* ast) {
    // MLIR setup
    _mlirCtxt.loadDialect<mlir::db::DB>();
    mlir::OpBuilder builder(&_mlirCtxt);
    mlir::Location loc = builder.getUnknownLoc();
    auto mainMod = mlir::ModuleOp::create(loc);

    { // Create main
        builder.setInsertionPointToEnd(mainMod.getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(&_mlirCtxt, {}, {});
        auto func = builder.create<mlir::func::FuncOp>(loc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        builder.setInsertionPointToStart(&block);
    }

    VariableDependencyGraph vdg;
    vdg.buildFromAST(ast);

    std::vector<VariableDependencyGraphTraversal::Visit> trav;
    VariableDependencyGraphTraversal vdgTrav;
    vdgTrav.computeTraversal(&vdg, trav);

    /*
    for (const DependencyEdge* e : trav) {
        const VariableDependency* src = e->src();
        // const VariableDependency* tgt = e->tgt();

        if (!_varMap.contains(src)) {
            const mlir::db::ColumnType col = createColumnFor(src);
            builder.create<mlir::db::ScanNodes>(loc, col);
        }

        // const EdgeMetadata::EdgeType type = e->data().type();
    }
    */
}
