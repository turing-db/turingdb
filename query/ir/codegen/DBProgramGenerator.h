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
    using EdgeProducerMap = std::unordered_map<const VariableDependency*, const VariableDependency*>;

    void generate(const CypherAST* ast, mlir::ModuleOp* module);

private:
    mlir::MLIRContext* _mlirCtxt {nullptr};
    mlir::OpBuilder* _opBuilder {nullptr};

    // Maps a Cypher variable to all of its MLIR variable uses, in order of appearence
    VariableIdentityMap _varMap;

    // Maps Cypher edge variables to the Cypher node variable which produced them
    // Can be the source or the target of the edge, depending on traversal
    EdgeProducerMap _edgeProdMap;

    void addScanNodes(const VariableDependency* var);
    void addGetOutEdges(const VariableDependency* src, const VariableDependency* edge, const VariableDependency* tgt);

    mlir::db::ColumnType allocColumnType(mlir::Type type);
    void registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val);
};

}
