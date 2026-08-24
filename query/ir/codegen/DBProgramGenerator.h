#pragma once

#include <memory>
#include <span>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "llvm/ADT/STLFunctionalExtras.h"
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
class SinglePartQuery;
class Stmt;
class WhereClause;
class WithStmt;

class DBProgramGenerator {
public:
    using VariableIdentities = std::vector<mlir::TypedValue<mlir::Type>>;
    using VariableIdentityMap = std::unordered_map<const VariableDependency*, VariableIdentities>;
    using DefinedVars = std::unordered_set<const VariableDependency*>;
    using ExprValueMap = std::unordered_map<const Expr*, mlir::Value>;
    using ProjectedColumnMap = std::unordered_map<const VarDecl*, mlir::Value>;
    using ColumnPredicate = llvm::function_ref<bool(mlir::Value)>;

    // Maps a Cypher variable name to the last column defined for it, the one the
    // projection and its ORDER BY read
    using VariableColumnMap = std::unordered_map<std::string_view, mlir::Value>;

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

    // A WITH drops this whole object rather than naming its members one at a time, so a
    // map added here is one the next part cannot read a stale binding out of
    struct PartScope {
        VariableIdentityMap _varMap;

        std::unordered_map<const VariableDependency*, mlir::Value> _edgeTypeMap;

        ExprValueMap _exprMap;

        // Maps each projected item to the column it produced, under the declaration the
        // alias of that item shares with it
        ProjectedColumnMap _projectedColumns;
    };

    PartScope _part;

    // Of one part too, but emptied rather than replaced: its variables and edges point
    // at each other
    VariableDependencyGraph _vdg;

    struct TranslatedComponent {
        std::unique_ptr<mlir::Region> _region;
        std::vector<const VariableDependency*> _vars;
        llvm::SmallVector<mlir::Value> _columns;
    };

    void generateQueryParts(const SinglePartQuery* query);

    // One query part: the statements between two WITH barriers
    void generatePart(std::span<Stmt* const> stmts);

    void generateTraversal(std::span<Stmt* const> stmts);

    // A barrier leaves behind the traversal an edge's column was published under, so a
    // pattern matching that edge again has nothing to join onto
    void throwOnRematchedBoundEdge() const;

    bool holdsColumn(const VariableDependency* var) const;

    // Rejects a pattern variable the traversal left without a column: a shape it cannot
    // walk from the variables in scope
    void throwOnUnboundPatternVariable() const;

    void closeBoundJoins(std::vector<const VariableDependency*>& carriedSet,
                         std::vector<const VariableDependency*>& dataflowVars);

    // Walks one hop whose far end already holds a column, so the hop constrains the rows
    // instead of fanning them out: the column it lands on is filtered against that one
    void closeBoundJoin(const DependencyEdge* edgeProducer,
                        const VariableDependency* edge,
                        const VariableDependency* target,
                        std::vector<const VariableDependency*>& carriedSet,
                        std::vector<const VariableDependency*>& dataflowVars);

    // The walk skips a merge target that already holds a column, so the equality closing
    // a cycle through a bound variable is left to be emitted here
    void closeBoundMerges();

    // Extends the dataflow the barrier left behind rather than opening one of its own,
    // filling @param dataflowVars with every variable that dataflow carries a column for
    void extendBoundDataflow(DefinedVars& defined,
                             std::vector<const VariableDependency*>& dataflowVars);

    // Turns the dataflow the barrier left behind into a cross product factor: every op
    // emitted into @param mainBlock so far, bar the ones binding a constant
    void takeBoundDataflow(mlir::Block* mainBlock, TranslatedComponent& component);

    // Adds filters for edges which should be equivalent (joined on)
    void resolveEdgeIdentities();

    // Translate a connected component of @ref _vdg, fills @param outVars
    void translateComponent(const VariableDependency* root,
                            std::unordered_set<const VariableDependency*>& defined,
                            std::vector<const VariableDependency*>& outVars);

    // Walks a component out from @param root, one edge traversal per hop. The root has to
    // hold a column already: translateComponent opens one with a scan, and a variable a
    // WITH bound carries the one the barrier published
    void expandComponent(const VariableDependency* root,
                         std::unordered_set<const VariableDependency*>& defined,
                         std::vector<const VariableDependency*>& carriedSet,
                         std::vector<const VariableDependency*>& outVars);

    // Converts an arbitrary number of connected components into a cascading nest of
    // CrossProducts
    void buildCrossProductCascade(std::vector<TranslatedComponent>& components,
                                  mlir::Block* targetBlock,
                                  llvm::SmallVectorImpl<mlir::Value>& results);

    // Moves a translated connected component into a CrossProduct factor
    void moveComponentToFactor(TranslatedComponent& component, mlir::Block* factorBlock);

    void generateCreate(const SinglePartQuery* query);
    void generateSet(const SinglePartQuery* query);
    void generateDelete(const SinglePartQuery* query);
    void generateOutput(const Projection* projection);

    void generateWith(const WithStmt* with);

    // A barrier publishes one row per row it read, not the single row its literals are:
    // this binds a projection of constants alone to the rows of the match under it
    void broadcastConstantProjection(llvm::SmallVectorImpl<mlir::Value>& projected);

    void publishBoundColumns(llvm::ArrayRef<llvm::StringRef> names,
                             llvm::ArrayRef<mlir::Value> columns);

    // The column every variable in scope currently holds, under its name: one per
    // traversal variable, plus one per edge identity under its representative
    void collectVariableColumns(VariableColumnMap& variableColumns) const;

    void translateProjection(const Projection* projection,
                             const VariableColumnMap& variableColumns,
                             llvm::SmallVectorImpl<mlir::Value>& projected,
                             llvm::SmallVectorImpl<llvm::StringRef>& names);

    void translateProjectionTail(const Projection* projection,
                                 const VariableColumnMap& variableColumns,
                                 llvm::SmallVectorImpl<mlir::Value>& projected);

    template <typename CutOp>
    void translateCut(const Projection* projection,
                      const Expr* countExpr,
                      std::string_view clauseName,
                      llvm::SmallVectorImpl<mlir::Value>& projected);

    void translateDistinct(const Projection* projection,
                           llvm::SmallVectorImpl<mlir::Value>& projected);

    // Dedups a projection of constants alone, whose rows are one row repeated, by
    // capping @param projected at the single row the dedup would keep
    void translateDistinctOverConstants(const Projection* projection,
                                        llvm::SmallVectorImpl<mlir::Value>& projected);

    void runPasses();

    void translateOrderBy(const Projection* projection,
                          const VariableColumnMap& variableColumns,
                          llvm::SmallVectorImpl<mlir::Value>& projected);

    // The column holding the values of an expression: the column already published for
    // the traversal variable it names, when the expression is nothing but that variable,
    // otherwise the column its translation computes
    mlir::Value getOrTranslateExprColumn(const VariableColumnMap& variableColumns, const Expr* expr);

    mlir::Value resolveEntityColumn(std::string_view varName);

    bool isRowAlignedHere(mlir::Value column) const;

    // Taken in the order the query declares its variables, so the choice is the query's
    // and not the addresses'
    mlir::Value resolveColumnInScope(ColumnPredicate accept) const;

    mlir::Value resolveRowCarryingColumn() const;

    // A scope of constants alone is the single row those constants are, which is what
    // count(*) counts there
    mlir::Value resolveWildcardColumn() const;

    void throwIfNoWildcardColumn(mlir::Value column) const;

    void generatePropertyConstraints(std::span<Stmt* const> stmts);
    void generateFilters(std::span<Stmt* const> stmts);

    void applyPredicateFilters(std::span<const Expr* const> predicates);

    void generateGroupAggregate(const Projection* projection);

    // Publishes the grouped column of every grouping key and aggregate result an ORDER BY
    // key reads, so the key computes over one value per group rather than over the
    // ungrouped column the group aggregate consumed
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

    // Such a scope is the single row those constants are: the predicate and the columns
    // are laid out over that row
    void filterConstantScope(mlir::Value predicate,
                             llvm::ArrayRef<const VariableDependency*> constantVars);

    void applyConstraints(const VariableDependency* var);

    void addMergeFilter(const VariableDependency* var,
                        std::vector<const VariableDependency*>& carriedSet);

    // @param joinedTarget is where the column the hop lands on is written when the caller
    // closes a join with it, and null when the hop binds it to @param tgt - the two forms
    // below, which are what callers reach for
    template<typename EdgeOp>
    void walkEdge(const VariableDependency* src,
                  const VariableDependency* edge,
                  const VariableDependency* tgt,
                  const std::vector<const VariableDependency*>& carrySet,
                  mlir::Value* joinedTarget);

    template<typename EdgeOp>
    void addEdgeTraversal(const VariableDependency* src,
                          const VariableDependency* edge,
                          const VariableDependency* tgt,
                          const std::vector<const VariableDependency*>& carrySet);

    // The target already holds a column, so the hop constrains the rows instead of
    // fanning them out: the column it lands on is returned for the caller to filter
    // against the one @param tgt keeps
    template<typename EdgeOp>
    mlir::Value addJoiningEdgeTraversal(const VariableDependency* src,
                                        const VariableDependency* edge,
                                        const VariableDependency* tgt,
                                        const std::vector<const VariableDependency*>& carrySet);

    void addGetOutEdges(const VariableDependency* src,
                        const VariableDependency* edge,
                        const VariableDependency* tgt,
                        const std::vector<const VariableDependency*>& carrySet) {
        addEdgeTraversal<mlir::db::GetOutEdges>(src, edge, tgt, carrySet);
    }

    void addGetInEdges(const VariableDependency* src,
                       const VariableDependency* edge,
                       const VariableDependency* tgt,
                       const std::vector<const VariableDependency*>& carrySet) {
        addEdgeTraversal<mlir::db::GetInEdges>(src, edge, tgt, carrySet);
    }

    void addGetEdges(const VariableDependency* src,
                     const VariableDependency* edge,
                     const VariableDependency* tgt,
                     const std::vector<const VariableDependency*>& carrySet) {
        addEdgeTraversal<mlir::db::GetEdges>(src, edge, tgt, carrySet);
    }

    mlir::db::ColumnType allocColumnType(mlir::Type type);
    void registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val);
};

}
