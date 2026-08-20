#pragma once

#include <memory>
#include <span>
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
class FunctionInvocationExpr;
class FunctionInvocation;
class UnaryExpr;
class CypherAST;
class Expr;
class Literal;
class ListLiteral;
class Projection;
class PropertyExpr;
class UnwindStmt;
class VarDecl;
class VariableDependency;
class DependencyEdge;

class DBProgramGenerator {
public:
    using VariableIdentities = std::vector<mlir::TypedValue<mlir::Type>>;
    using VariableIdentityMap = std::unordered_map<const VariableDependency*, VariableIdentities>;
    using DefinedVars = std::unordered_set<const VariableDependency*>;
    using ExprValueMap = std::unordered_map<const Expr*, mlir::Value>;
    using ProjectedColumnMap = std::unordered_map<const VarDecl*, mlir::Value>;

    // Maps a Cypher variable name to the last column defined for it, the one the
    // projection and its ORDER BY read
    using VariableColumnMap = std::unordered_map<std::string_view, mlir::Value>;

    // Each grouping key expression of an aggregating projection beside the column the
    // group aggregate reduced it to, one value per group
    using GroupedKeyColumns = llvm::SmallVector<std::pair<const Expr*, mlir::Value>>;

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

    ExprValueMap _exprMap;

    // Maps each projected item to the column it produced, under the declaration the
    // alias of that item shares with it
    ProjectedColumnMap _projectedColumns;

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
    void generateSet(const CypherAST* ast);
    void generateDelete(const CypherAST* ast);
    void generateOutput(const CypherAST* ast);

    // Dedups the projection with a RemoveDuplicates over the varying columns of
    // @param projected, replacing each of them with its deduped counterpart
    void translateDistinct(const Projection* projection,
                           llvm::SmallVectorImpl<mlir::Value>& projected);

    // Dedups a projection of constants alone, whose rows are one row repeated, by
    // capping @param projected at the single row the dedup would keep
    void translateDistinctOverConstants(const Projection* projection,
                                        llvm::SmallVectorImpl<mlir::Value>& projected);

    void runPasses();

    // Reorders the projection with a Sort over @param projected, replacing each
    // projected column with its sorted counterpart
    void translateOrderBy(const Projection* projection,
                          const VariableColumnMap& variableColumns,
                          llvm::SmallVectorImpl<mlir::Value>& projected);

    // The column holding the values of an expression: the column already published for
    // the traversal variable it names, when the expression is nothing but that variable,
    // otherwise the column its translation computes
    mlir::Value getOrTranslateExprColumn(const VariableColumnMap& variableColumns, const Expr* expr);

    mlir::Value resolveEntityColumn(std::string_view varName);

    bool isRowAlignedHere(mlir::Value column) const;

    // The column count(*) counts: the first variable of the pattern bound to a column
    // holding the rows flowing past the insertion point, taken in the order the query
    // declares its variables so the choice is the query's and not the addresses'
    mlir::Value resolveWildcardColumn() const;

    void generatePropertyConstraints(const CypherAST* ast);
    void generateFilters(const CypherAST* ast);

    void applyPredicateFilters(std::span<const Expr* const> predicates);

    void generateGroupAggregate(const CypherAST* ast);

    // Publishes the grouped column of every grouping key an ORDER BY key reads, so that
    // translating the key computes over one value per group instead of re-reading the
    // ungrouped column the group aggregate consumed
    void bindOrderByKeyColumns(const Projection* projection, const GroupedKeyColumns& groupedKeys);
    void bindGroupedKeyColumn(const Expr* expr, const GroupedKeyColumns& groupedKeys);

    bool collectAggregateInvocations(const Expr* expr,
                                     llvm::SmallVectorImpl<const FunctionInvocationExpr*>& found);

    void translateExpr(const Expr* expr);
    void translateUnaryExpr(const Expr* expr, const UnaryExpr* unaryExpr);
    void translateBinaryExpr(const Expr* expr, const BinaryExpr* binExpr);
    void translateFunctionInvocationExpr(const Expr* expr, const FunctionInvocationExpr* funcExpr);

    void translateFunctionExpr(const Expr* expr, const FunctionInvocation* invocation);

    mlir::Value translateArg(const Expr* argExpr);
    mlir::Value translateLiteralExpr(const Literal* literal);
    mlir::Value translatePropertyExpr(const PropertyExpr* propExpr);

    // The attribute carrying a scalar literal's value and type, or a null attribute for
    // any other literal kind
    mlir::TypedAttr scalarLiteralAttr(const Literal* literal);

    // Fills one attribute per element of an UNWIND list, each keeping its literal's type
    void translateUnwindElements(const ListLiteral* list,
                                 llvm::SmallVectorImpl<mlir::Attribute>& elements);

    // The attribute one UNWIND list element unwinds from: a typed attribute for a scalar,
    // a unit attribute for a null, a nested array attribute for a nested list
    mlir::Attribute unwindElementAttr(const Literal* literal);

    void addScanNodes(const VariableDependency* var);

    // Opens a dataflow from an UNWIND's literal list, the way addScanNodes opens one
    // from the graph's nodes
    void addUnwindConst(const VariableDependency* var, const UnwindStmt* unwind);
    void filterAllColumns(mlir::Value predicate);

    void applyConstraints(const VariableDependency* var);

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

    template <typename... Args>
    void addGetEdges(Args&&... args) {
        return addEdgeTraversal<mlir::db::GetEdges>(std::forward<Args>(args)...);
    }

    mlir::db::ColumnType allocColumnType(mlir::Type type);
    void registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val);
};

}
