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
class MatchStmt;
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

    // The traversal root a CALL ahead of the MATCH bound, when the query lets the traversal
    // expand that column instead of scanning the graph and joining. Null otherwise, which
    // leaves every call after the traversal and every root opening with a scan.
    const VariableDependency* _drivenRoot {nullptr};

    VariableDependencyGraph _vdg;

    struct TranslatedComponent {
        std::unique_ptr<mlir::Region> _region;
        std::vector<const VariableDependency*> _vars;
        llvm::SmallVector<mlir::Value> _columns;
    };

    // The columns in flight at the current insertion point, and what each belongs to so the
    // results of an op taking them all can be rebound: the latest value of every Cypher
    // variable, then every edge-type column, then every column a CALL yielded.
    struct InFlightColumns {
        llvm::SmallVector<mlir::Value> _columns;
        llvm::SmallVector<const VariableDependency*> _variables;
        llvm::SmallVector<const VariableDependency*> _edgeTypeVariables;
        llvm::SmallVector<size_t> _yieldedIndices;
    };

    // Generates the calls the query writes ahead of its first MATCH, when what they yield
    // can drive the traversal, and names the root that will expand it. Generates nothing
    // otherwise: a call whose rows the traversal does not consume has to be paired with it,
    // which is what generating it after the traversal does.
    void generateLeadingCalls(const CypherAST* ast);

    // Emits the op of a system-level statement - LOAD GRAPH, CHANGE, COMMIT and
    // their siblings - which is the whole program. False for an ordinary query,
    // which then goes through the traversal pipeline
    bool generateSystemCommand(const CypherAST* ast);

    void generateTraversal(const CypherAST* ast);

    // Whether a variable can open a connected component: a node variable no traversal binds
    bool isValidRoot(const VariableDependency& var) const;

    // The variable each connected component of the graph opens with - the roots
    // translateComponent is called with, one per component
    void collectComponentRoots(llvm::SmallVectorImpl<const VariableDependency*>& roots) const;

    // Adds filters for edges which should be equivalent (joined on)
    void resolveEdgeIdentities();

    // Joins each pattern variable to the column a CALL yielded under its name: the two are
    // one Cypher variable, and the call has already paired its rows with the ones the
    // pattern matched, so keeping the rows where the two columns agree is that join.
    void resolveYieldedIdentities();

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

    // Emits each MATCH's WHERE and each CALL in the order the query writes them, so a
    // predicate reading what a call yielded is generated once the call has bound it.
    void generateFiltersAndCalls(const CypherAST* ast);
    void generateMatchFilter(const MatchStmt* matchStmt);
    void generateCall(const CallStmt* callStmt);

    // Generate the filter a CALL's YIELD ... WHERE asks for, over everything in flight once
    // the call has run - so the predicate reads the rows the procedure emitted.
    void generateYieldFilter(const YieldItems* yieldItems);

    void generateCrossedCall(std::string_view procedureName,
                             llvm::ArrayRef<mlir::Attribute> yieldedNames,
                             llvm::ArrayRef<std::string_view> yieldedVariables,
                             mlir::ValueRange inputs,
                             const InFlightColumns& inFlight);

    // Whether a column holds the rows flowing past the current insertion point, which is
    // what an op consuming a whole row set needs of each of its operands.
    bool isRowAlignedHere(mlir::Value column) const;

    // Collect those columns. One bound in another block is skipped: an op here can only
    // take what this block binds.
    void collectInFlightColumns(InFlightColumns& inFlight);

    // Rebind them to an op's results, so later ops read what it produced. firstResult is
    // where its pass-through results start - zero for an op that only takes them (a
    // filter), past its own results for one that adds some (a call).
    void rebindInFlightColumns(mlir::Operation::result_range results,
                               size_t firstResult,
                               const InFlightColumns& inFlight);

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

    // The column count(*) counts: the first variable of the pattern bound to a column
    // holding the rows flowing past the insertion point, taken in the order the query
    // declares its variables so the choice is the query's and not the addresses'
    mlir::Value resolveWildcardColumn() const;

    // The column an aggregate folds over: the column count(*) counts, or the one its
    // argument's translation computes
    mlir::Value translateAggregateInput(const Expr* argExpr);

    void generatePropertyConstraints(const CypherAST* ast);

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

    // Opens a dataflow from the column a CALL yielded, the way addScanNodes opens one from
    // the graph's nodes: the variable takes those rows over as its own
    void addYieldedColumn(const VariableDependency* var, mlir::Value column);

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
    void rebindYieldedColumn(std::string_view name, mlir::TypedValue<mlir::Type> val);
};

}
