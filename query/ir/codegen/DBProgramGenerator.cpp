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

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

mlir::db::ColumnType DBProgramGenerator::createColumnFor(const VariableDependency* var) {
    const auto& existingVars = _varMap[var];
    const std::string colName = std::string(var->getName()) + std::to_string(existingVars.size());
    const mlir::db::ColumnType col = mlir::db::ColumnType::get(_mlirCtxt, colName);
    _varMap[var].emplace_back(col);

    return col;
}

void DBProgramGenerator::generate(const CypherAST* ast, mlir::ModuleOp* module) {
    _mlirCtxt->loadDialect<mlir::db::DB>();
    mlir::OpBuilder builder(_mlirCtxt);
    const mlir::Location loc = builder.getUnknownLoc();

    { // Create main
        builder.setInsertionPointToEnd(module->getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, {}, {});
        auto func = builder.create<mlir::func::FuncOp>(loc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        builder.setInsertionPointToStart(&block);
    }

    VariableDependencyGraph vdg;
    vdg.buildFromAST(ast);

    std::vector<VariableDependencyGraphTraversal::Visit> trav;
    VariableDependencyGraphTraversal vdgTrav;
    vdgTrav.computeTraversal(&vdg, trav);

    for (const auto& node : trav) {
        const VariableDependency* var = node._var;
        const auto generatedBy =  node._gen;

        switch (generatedBy) {
            case VariableDependencyGraphTraversal::Generator::SCAN_NODES: {
                bioassert(!_varMap.contains(var), "Visited node without producer");
                const mlir::db::ColumnType col = createColumnFor(var);
                builder.create<mlir::db::ScanNodes>(loc, col);
            }
            break;

            case VariableDependencyGraphTraversal::Generator::GET_OUT_EDGES:
                throw FatalException("GET_OUT_EDGES not supported.");
                bioassert(_varMap.contains(var), "Missing source.");
                // const mlir::db::ColumnType col = createColumnFor(var);
            break;

            case VariableDependencyGraphTraversal::Generator::GET_IN_EDGES:
                throw FatalException("GET_IN_EDGES not supported.");
            break;

            case VariableDependencyGraphTraversal::Generator::GET_EDGES:
                throw FatalException("GET_EDGES not supported.");
            break;

            case VariableDependencyGraphTraversal::Generator::MERGE:
                throw FatalException("MERGE not supported.");
            break;

            case VariableDependencyGraphTraversal::Generator::_SIZE:
                throw FatalException("Invalid traversal type.");
            break;

        }
    }
    builder.create<mlir::func::ReturnOp>(loc);
}
