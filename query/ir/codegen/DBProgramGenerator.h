#pragma once

#include <unordered_map>

#include "DBTypes.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

namespace db {

class CypherAST;
class VariableDependency;

class DBProgramGenerator {
public:
    using VariableIdentities = std::vector<mlir::TypedValue<mlir::db::ColumnType>>;
    using VariableIdentityMap = std::unordered_map<const VariableDependency*, VariableIdentities>;

    DBProgramGenerator(mlir::MLIRContext* ctxt, mlir::OpBuilder* bld)
        : _mlirCtxt(ctxt),
        _builder(bld)
    {
    }

    void generate(const CypherAST* ast, mlir::ModuleOp* module);

private:
    mlir::MLIRContext* _mlirCtxt {nullptr};
    mlir::OpBuilder* _builder {nullptr};

    VariableIdentityMap _varMap;

    mlir::db::ColumnType allocColumnType(const VariableDependency* var);
    void registerValue(const VariableDependency* var, mlir::TypedValue<mlir::db::ColumnType> val);
    mlir::TypedValue<mlir::db::ColumnType> getMostRecentColumnFor(const VariableDependency* var);
};
    
}
