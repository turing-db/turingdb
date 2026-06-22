#include "DBProgramGenerator.h"

#include "CypherAST.h"

#include "DBTypes.h"
#include "DependencyEdge.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"
#include "VariableDependencyGraphTraversal.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Location.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Types.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"

#include "DBOps.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "llvm/ADT/SmallVector.h"

using namespace db;

mlir::db::ColumnType DBProgramGenerator::allocColumnType(const VariableDependency* var) {
    const std::string colName = std::string(var->getName()) + std::to_string(_varMap[var].size());
    return mlir::db::ColumnType::get(_mlirCtxt, colName);
}

void DBProgramGenerator::registerValue(const VariableDependency* var, mlir::TypedValue<mlir::db::ColumnType> val) {
    _varMap[var].emplace_back(val);
}

mlir::TypedValue<mlir::db::ColumnType> DBProgramGenerator::getMostRecentColumnFor(const VariableDependency* var) {
    bioassert(_varMap.contains(var), "Tried to get column for unregistered variable.");
    return _varMap[var].back();
}

void DBProgramGenerator::generate(const CypherAST* ast, mlir::ModuleOp* module) {
    _mlirCtxt->loadDialect<mlir::db::DB>();
    const mlir::Location loc = _builder->getUnknownLoc();

    { // Create main
        _builder->setInsertionPointToEnd(module->getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, {}, {});
        auto func = _builder->create<mlir::func::FuncOp>(loc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        _builder->setInsertionPointToStart(&block);
    }

    VariableDependencyGraph vdg;
    vdg.buildFromAST(ast);

    std::vector<VariableDependencyGraphTraversal::Visit> trav;
    VariableDependencyGraphTraversal vdgTrav;
    vdgTrav.computeTraversal(&vdg, trav);

    llvm::SmallVector<mlir::TypedValue<mlir::db::ColumnType>, 10> out;

    for (const auto& node : trav) {
        const VariableDependency* var = node._var;
        const VariableDependencyGraphTraversal::Generator generatedBy = node._gen;
        const VariableDependency* fstProd = node._fstProducer;

        switch (generatedBy) {
            case VariableDependencyGraphTraversal::Generator::SCAN_NODES: {
                bioassert(!_varMap.contains(var), "Visited node without producer");

                const mlir::db::ColumnType col = allocColumnType(var);
                auto scan = _builder->create<mlir::db::ScanNodes>(loc, col);

                registerValue(var, scan.getResult());
                out.push_back(scan.getResult());
            }
            break;

            case VariableDependencyGraphTraversal::Generator::GET_OUT_EDGES: {
                bioassert(_varMap.contains(fstProd), "Missing source.");
                // Placeholder — the actual op is emitted in GET_EDGE_TGT once both
                // the edge variable and target variable are known.
            }
            break;

            case VariableDependencyGraphTraversal::Generator::GET_IN_EDGES:
                throw FatalException("GET_IN_EDGES not supported.");
            break;

            case VariableDependencyGraphTraversal::Generator::GET_EDGES:
                throw FatalException("GET_EDGES not supported.");
            break;

            case VariableDependencyGraphTraversal::Generator::GET_EDGE_TGT: {
                const mlir::TypedValue<mlir::db::ColumnType> srcValue = getMostRecentColumnFor(fstProd);

                const mlir::db::ColumnType newSrcColType = allocColumnType(fstProd);
                const mlir::db::ColumnType tgtColType = allocColumnType(var);
                const mlir::db::ColumnType edgeIDCol = mlir::db::ColumnType::get(_mlirCtxt, "temp");
                const mlir::db::ColumnType edgeTypeCol = mlir::db::ColumnType::get(_mlirCtxt, "tempET");

                auto edges = _builder->create<mlir::db::GetOutEdges>(
                    loc,
                    mlir::TypeRange {newSrcColType, edgeIDCol, edgeTypeCol, tgtColType},
                    mlir::ValueRange {srcValue});

                registerValue(fstProd, edges.getSrcids());
                registerValue(var, edges.getTgtids());

                for (auto& outVal : out) {
                    if (outVal == srcValue) {
                        outVal = edges.getSrcids();
                        break;
                    }
                }
                out.push_back(edges.getTgtids());
            }
            break;

            case VariableDependencyGraphTraversal::Generator::GET_EDGE_SRC:
                throw FatalException("GET_EDGE_SRC not supported.");
            break;

            case VariableDependencyGraphTraversal::Generator::MERGE:
                throw FatalException("MERGE not supported.");
            break;

            case VariableDependencyGraphTraversal::Generator::_SIZE:
                throw FatalException("Invalid traversal type.");
            break;
        }
    }
    bioassert(!out.empty(), "nothing to output");

    _builder->create<mlir::db::Output>(
        loc, mlir::ValueRange {out.begin(), std::distance(out.begin(), out.end())});
    _builder->create<mlir::func::ReturnOp>(loc);
}
