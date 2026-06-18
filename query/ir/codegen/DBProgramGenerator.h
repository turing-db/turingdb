#pragma once

#include <unordered_map>

#include "DBTypes.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Value.h"

namespace db {

class CypherAST;
class VariableDependency;

class DBProgramGenerator {
public:
    using VariableIdentities = std::vector<mlir::db::ColumnType>;
    using VariableIdentityMap = std::unordered_map<const VariableDependency*, VariableIdentities>;

    DBProgramGenerator()
        : _builder(&_mlirCtxt)
    {
    }

    void generate(const CypherAST* ast);
private:
    mlir::MLIRContext _mlirCtxt;
    mlir::Builder _builder;

    VariableIdentityMap _varMap;

    mlir::db::ColumnType createColumnFor(const VariableDependency* var);
};
    
}
