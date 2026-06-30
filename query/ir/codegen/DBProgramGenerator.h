#pragma once

#include <unordered_map>
#include <vector>

#include "mlir/IR/Types.h"
#include "mlir/IR/Value.h"

#include "DBTypes.h"

namespace mlir {
class ModuleOp;
class OpBuilder;
}

namespace db {

class CypherAST;
class VariableDependency;

class DBProgramGenerator {
public:
    using VariableIdentities = std::vector<mlir::TypedValue<mlir::Type>>;
    using VariableIdentityMap = std::unordered_map<const VariableDependency*, VariableIdentities>;

    void generate(const CypherAST* ast, mlir::ModuleOp* module);
private:
    mlir::MLIRContext* _mlirCtxt {nullptr};
    mlir::OpBuilder* _opBuilder {nullptr};

    VariableIdentityMap _varMap;

    void addScanNodes(const VariableDependency* var);
    void addGetOutEdges(const VariableDependency* src, const VariableDependency* edge, const VariableDependency* tgt);

    mlir::db::ColumnType allocColumnType(mlir::Type type);
    void registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val);
};

}
