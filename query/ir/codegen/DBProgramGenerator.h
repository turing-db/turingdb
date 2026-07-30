#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "llvm/ADT/SmallVector.h"
#include "mlir/IR/Types.h"
#include "mlir/IR/Value.h"

#include "DBOps.h"
#include "DBTypes.h"

#include "VariableDependencyGraph.h"

namespace mlir {
class ModuleOp;
class OpBuilder;
class Block;
class Region;
}

namespace db {

class BinaryExpr;
class UnaryExpr;
class CypherAST;
class Expr;
class Literal;
class Projection;
class PropertyExpr;
class VariableDependency;
class DependencyEdge;

class DBProgramGenerator {
public:
    using VariableIdentities = std::vector<mlir::TypedValue<mlir::Type>>;
    using VariableIdentityMap = std::unordered_map<const VariableDependency*, VariableIdentities>;
    using DefinedVars = std::unordered_set<const VariableDependency*>;
    using ExprValueMap = std::unordered_map<const Expr*, mlir::Value>;

    // Maps a Cypher variable name to the last column defined for it, the one the
    // projection and its ORDER BY read
    using FinalIdentityMap = std::unordered_map<std::string_view, mlir::Value>;

    explicit DBProgramGenerator(mlir::ModuleOp* mainModule);
    ~DBProgramGenerator();

    void generate(const CypherAST* ast);

private:
    mlir::ModuleOp* _module {nullptr};
    mlir::MLIRContext* _mlirCtxt {nullptr};
    mlir::OpBuilder _opBuilder;

    // Maps a Cypher variable to all of its MLIR variable defs, in order of appearance
    VariableIdentityMap _varMap;

    // Maps each edge VDG var to its MLIR type column
    std::unordered_map<const VariableDependency*, mlir::Value> _edgeTypeMap;

    // Maps each WHERE clause expression to the MLIR value it produces
    ExprValueMap _exprMap;

    VariableDependencyGraph _vdg;

    struct TranslatedComponent {
        std::unique_ptr<mlir::Region> _region;
        std::vector<const VariableDependency*> _vars;
        llvm::SmallVector<mlir::Value> _columns;
    };

    void generateTraversal(const CypherAST* ast);

    // Adds filters for edges which should be equivalent (joined on)
    void resolveEdgeIdentities();

    // Translate a connected component of @ref _vdg, fills @param outVars
    void translateComponent(const VariableDependency* root,
                            std::unordered_set<const VariableDependency*>& defined,
                            std::vector<const VariableDependency*>& outVars);

    // Converts an arbitrary number of connected components into a cascading nest of
    // CrossProducts
    void buildCrossProductCascade(std::vector<TranslatedComponent>& components,
                                  mlir::Block* targetBlock,
                                  llvm::SmallVectorImpl<mlir::Value>& results);

    // Moves a translated connected component into a CrossProduct factor
    void moveComponentToFactor(TranslatedComponent& component, mlir::Block* factorBlock);

    void generateCreate(const CypherAST* ast);
    void generateOutput(const CypherAST* ast);

    // Reorders the projection with a Sort over @param projected, replacing each
    // projected column with its sorted counterpart
    void translateOrderBy(const Projection* projection,
                          const FinalIdentityMap& identities,
                          llvm::SmallVectorImpl<mlir::Value>& projected);

    // The column an expression reads: the traversal variable it names, when it names
    // one, otherwise the column its translation produces
    mlir::Value resolveExprColumn(const FinalIdentityMap& identities, const Expr* expr);

    void generatePropertyConstraints(const CypherAST* ast);
    void generateLabelConstraints(const CypherAST* ast);
    void generateEdgeTypeConstraints(const CypherAST* ast);
    void generateFilters(const CypherAST* ast);

    void translateExpr(const Expr* expr);
    void translateUnaryExpr(const Expr* expr, const UnaryExpr* unaryExpr);
    void translateBinaryExpr(const Expr* expr, const BinaryExpr* binExpr);
    mlir::Value translateLiteralExpr(const Literal* literal);
    mlir::Value translatePropertyExpr(const PropertyExpr* propExpr);

    void addScanNodes(const VariableDependency* var);
    void filterAllColumns(mlir::Value predicate);

    void addMergeFilter(const VariableDependency* var,
                        std::vector<const VariableDependency*>& carriedSet);

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
