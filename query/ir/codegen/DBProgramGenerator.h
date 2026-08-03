#pragma once

#include <memory>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "llvm/ADT/ArrayRef.h"
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
class CallStmt;
class CypherAST;
class Expr;
class Literal;
class ListLiteral;
class Projection;
class PropertyExpr;
class ReturnStmt;
class UnwindStmt;
class VarDecl;
class VariableDependency;
class DependencyEdge;
class YieldItems;

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
    // The columns a grouped aggregation produces, each under the expression it computes:
    // the grouping keys and the aggregate results
    using GroupedColumns = llvm::SmallVector<std::pair<const Expr*, mlir::Value>>;

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

    // The column a CALL produced for each of its yielded return values, named as the
    // query knows it - its alias when the YIELD renamed it. A yielded variable appears in
    // no pattern, so it is not a VDG variable and cannot live in _varMap; the projection
    // and symbol translation consult both. Kept in yield order, because a standalone CALL
    // has no projection of its own and emits its columns in that order.
    using YieldedColumn = std::pair<std::string_view, mlir::Value>;
    std::vector<YieldedColumn> _yieldedColumns;

    // Columns a CALL dropped from its carry set because nothing read them again. Their last
    // value belongs to a loop the dataflow has left, so collectLiveColumns must skip them.
    std::unordered_set<const VariableDependency*> _deadVariables;
    std::unordered_set<size_t> _deadYieldedColumns;

    VariableDependencyGraph _vdg;

    struct TranslatedComponent {
        std::unique_ptr<mlir::Region> _region;
        std::vector<const VariableDependency*> _vars;
        llvm::SmallVector<mlir::Value> _columns;
    };

    // The columns live at the current insertion point, and what each belongs to so the
    // results of an op taking them all can be rebound: the latest value of every Cypher
    // variable, then every edge-type column, then every column a CALL yielded.
    struct LiveColumns {
        llvm::SmallVector<mlir::Value> _columns;
        llvm::SmallVector<const VariableDependency*> _variables;
        llvm::SmallVector<const VariableDependency*> _edgeTypeVariables;
        llvm::SmallVector<size_t> _yieldedIndices;
    };

    // Cypher variable names, as the query writes them - the key the projection resolves a
    // returned variable by, so it is what a liveness question is asked in.
    using VariableNames = std::unordered_set<std::string_view>;

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

    // Generate a db.call_procedure for each CALL of the query, after the traversal and
    // its filters, so a call reads the rows the MATCH has narrowed to. The call's
    // arguments are its argument expressions' columns, its yielded return values become
    // the columns the projection reads, and the rows already in flight ride through its
    // carry set so they stay aligned with what the procedure emitted.
    void generateCalls(const CypherAST* ast);
    void generateCall(const CallStmt* callStmt, const VariableNames& usedAfterCall);

    // Generate the filter a CALL's YIELD ... WHERE asks for, over everything in flight once
    // the call has run - so the predicate reads the rows the procedure emitted.
    void generateYieldFilter(const YieldItems* yieldItems);

    // The variable names the query still reads once a call has run: the arguments of the
    // calls written after it, and whatever the projection returns. Nothing else can read a
    // column by then - the traversal, its constraints and its filters are all generated
    // before the calls - so a name absent here belongs to a column that is dead at the
    // call and need not be carried past it.
    void collectNamesUsedAfterCall(const CallStmt* call,
                                   llvm::ArrayRef<const CallStmt*> laterCalls,
                                   const ReturnStmt* returnStmt,
                                   VariableNames& used) const;

    // Add the variable names a call's YIELD ... WHERE reads. That filter runs after the
    // call, so those names outlive it even when nothing else reads them.
    void collectYieldWhereNames(const CallStmt* call, VariableNames& used) const;

    // Add every variable name an expression reads, walking into its sub-expressions, so a
    // projection item or a call argument contributes each variable it is built from.
    void collectExprNames(const Expr* expr, VariableNames& used) const;

    // Drop from a live set the columns no name in `usedAfterCall` claims, leaving the
    // groups in the order rebindLiveColumns walks them. Each one dropped is recorded as
    // dead, so it stops being in flight for every later op too.
    void dropUnusedLiveColumns(const VariableNames& usedAfterCall, LiveColumns& live);

    // Generate a call that reads none of the rows already in flight: it produces the same
    // rows for every one of them, so the two are crossed. What the query has matched so
    // far moves into one factor of a db.cross_product and the call becomes the other,
    // which is the shape a disconnected pattern already lowers to.
    void generateCrossedCall(std::string_view procedureName,
                             llvm::ArrayRef<mlir::Attribute> yieldedNames,
                             llvm::ArrayRef<std::string_view> yieldedVariables,
                             const LiveColumns& live);

    // Collect those columns. One bound in another block is skipped: an op here can only
    // take what this block binds.
    void collectLiveColumns(LiveColumns& live);

    // Rebind them to an op's results, so later ops read what it produced. firstResult is
    // where its pass-through results start - zero for an op that only takes them (a
    // filter), past its own results for one that adds some (a call).
    void rebindLiveColumns(mlir::Operation::result_range results,
                           size_t firstResult,
                           const LiveColumns& live);

    // The column a yielded variable of this name holds, or a null Value when no CALL
    // yielded it.
    mlir::Value findYieldedColumn(std::string_view name) const;

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

    // Publishes the grouped column of every grouping key and aggregate result an ORDER BY
    // key reads, so that translating the key computes over one value per group instead of
    // re-reading the ungrouped column the group aggregate consumed
    void bindOrderByKeyColumns(const Projection* projection, const GroupedColumns& groupedColumns);
    void bindGroupedKeyColumn(const Expr* expr, const GroupedColumns& groupedColumns);

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

    // Fills one attribute per element of a literal list, each keeping its literal's type
    void translateListElements(const ListLiteral* list,
                               llvm::SmallVectorImpl<mlir::Attribute>& elements);

    // The attribute one list element rides on: a typed attribute for a scalar, a unit
    // attribute for a null, a nested array attribute for a nested list
    mlir::Attribute listElementAttr(const Literal* literal);

    // The column holding a list literal's value: the one list, standing for every row
    mlir::Value translateListLiteral(const ListLiteral* list);

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
