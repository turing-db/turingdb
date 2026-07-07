#pragma once

#include <unordered_map>
#include <vector>

#include "mlir/IR/Types.h"
#include "mlir/IR/Value.h"

#include "DBOps.h"
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

    // Maps a Cypher variable to all of its MLIR variable uses, in order of appearence
    VariableIdentityMap _varMap;

    void generateTraversal(const CypherAST* ast);

    void generateOutput(const CypherAST* ast);

    void addScanNodes(const VariableDependency* var);

    template<typename EdgeOp>
    void addEdgeTraversal(const VariableDependency* src,
                          const VariableDependency* edge,
                          const VariableDependency* tgt,
                          const std::vector<const VariableDependency*>& carrySet);

    template <typename... Args>
    void addGetOutEdges(Args&&... args) {
        return addEdgeTraversal<mlir::db::GetOutEdges>(std::forward<Args>(args)...);
    }

    template <typename... Args>
    void addGetInEdges(Args&&... args) {
        return addEdgeTraversal<mlir::db::GetInEdges>(std::forward<Args>(args)...);
    }

    mlir::db::ColumnType allocColumnType(mlir::Type type);
    void registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val);
};

}
