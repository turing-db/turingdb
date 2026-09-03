#pragma once

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "llvm/ADT/ArrayRef.h"
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
class CallStmt;
class CypherAST;
class EmbeddingLiteral;
class Expr;
class Literal;
class ListLiteral;
class MatchStmt;
class Projection;
class PropertyExpr;
class ReturnStmt;
class UnwindStmt;
class VarDecl;
class VectorSearchStmt;
class VariableDependency;
class DependencyEdge;
class SinglePartQuery;
class Stmt;
class WhereClause;
class WithStmt;
class YieldClause;
class YieldItems;

class DBProgramGenerator {
public:
    using VariableIdentities = std::vector<mlir::TypedValue<mlir::Type>>;
    using VariableIdentityMap = std::unordered_map<const VariableDependency*, VariableIdentities>;
    using DefinedVars = std::unordered_set<const VariableDependency*>;
    using ExprValueMap = std::unordered_map<const Expr*, mlir::Value>;
    using ProjectedColumnMap = std::unordered_map<const VarDecl*, mlir::Value>;
    using ColumnPredicate = llvm::function_ref<bool(mlir::Value)>;
    using VariableColumnBinding = llvm::function_ref<void(const VarDecl*, std::string_view, mlir::Value)>;

    // Maps a Cypher variable to the last column defined for it, the one the projection
    // and its ORDER BY read
    using VariableColumnMap = std::unordered_map<const VarDecl*, mlir::Value>;

    // A column a CALL produced for one of its yielded return values, under the declaration
    // the query knows it by and the name it is output under. A standalone call naming no
    // YIELD declares nothing, so its columns carry a null declaration.
    struct YieldedColumn {
        const VarDecl* _decl {nullptr};
        std::string_view _name;
        mlir::Value _column;
    };

    // A column a part cut hands to the part below it, under the declaration and the name
    // it was bound to above. The name is owned, since publishing clears the variables the
    // one in flight points into.
    struct PublishedColumn {
        const VarDecl* _decl {nullptr};
        std::string _name;
        mlir::Value _column;
    };

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

        // The column a CALL produced for each of its yielded return values. A yielded
        // variable appears in no pattern, so it is not a VDG variable and cannot live in
        // _varMap; the projection and symbol translation consult both. Kept in yield
        // order, because a standalone CALL has no projection of its own and emits its
        // columns in that order.
        std::vector<YieldedColumn> _yieldedColumns;

        // What a CREATE wrote for one of its named pattern entities: the column of
        // provisional IDs, and the value column of every property it set. A created entity
        // is in no pattern the traversal walked, so it is no VDG variable, and its rows are
        // not in the graph a fetch would read - both come from here instead
        struct CreatedEntity {
            mlir::Value _column;
            std::unordered_map<std::string_view, mlir::Value> _properties;
        };

        std::unordered_map<const VarDecl*, CreatedEntity> _createdEntities;

        // The traversal root a CALL ahead of the MATCH bound, when the query lets the
        // traversal expand that column instead of scanning the graph and joining. Null
        // otherwise, which leaves every call after the traversal and every root opening
        // with a scan.
        const VariableDependency* _drivenRoot {nullptr};

        // The pattern variables a const scan opened from an UNWIND's node IDs. A seeded
        // variable the traversal reached some other way holds rows that are not the listed
        // nodes, so what it was seeded with has to be checked against what was walked
        std::unordered_set<const VariableDependency*> _seededVars;
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

    // Whether this statement closes a part on the cut it carries: a MATCH whose ORDER BY,
    // SKIP or LIMIT reads the rows that MATCH produced, which a later MATCH of the same
    // part would otherwise have crossed into them first
    bool closesPartOnItsCut(const Stmt* stmt, std::span<Stmt* const> following) const;

    void generateTraversal(std::span<Stmt* const> stmts);

    // A barrier leaves behind the traversal an edge's column was published under, so a
    // pattern matching that edge again has nothing to join onto
    void throwOnRematchedBoundEdge() const;

    bool holdsColumn(const VariableDependency* var) const;

    // Rejects a pattern variable the traversal left without a column: a shape it cannot
    // walk from the variables in scope
    void throwOnUnboundPatternVariable() const;

    // Rejects a variable an UNWIND seeds with node IDs that the traversal reached some
    // other way, its list left unmatched: only the node a component opens from is seeded
    void throwOnDroppedUnwindSeed() const;

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

    // The columns in flight at the current insertion point, and what each belongs to so the
    // results of an op taking them all can be rebound: the latest value of every Cypher
    // variable, then every edge-type column, then every column a CALL yielded.
    struct InFlightColumns {
        llvm::SmallVector<mlir::Value> _columns;
        llvm::SmallVector<const VariableDependency*> _variables;
        llvm::SmallVector<const VariableDependency*> _edgeTypeVariables;
        llvm::SmallVector<size_t> _yieldedIndices;
    };

    // Generates the statements a part writes ahead of its first MATCH that bind variables
    // of their own - its calls and its vector searches - when what they yield can drive the
    // traversal, and names the root that will expand it. Generates nothing otherwise: rows
    // the traversal does not consume have to be paired with it, which is what generating
    // them after the traversal does.
    void generateLeadingYields(std::span<Stmt* const> stmts);

    // Emits the op of a system-level statement - LOAD GRAPH, CHANGE, COMMIT and
    // their siblings - which is the whole program. False for an ordinary query,
    // which then goes through the traversal pipeline
    bool generateSystemCommand(const CypherAST* ast);

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

    // Records what a CREATE wrote for one named entity of its pattern, so the projection
    // reads that back rather than fetching an ID the graph does not hold yet
    void publishCreatedEntity(const VarDecl* decl,
                              mlir::Value column,
                              llvm::ArrayRef<llvm::StringRef> propNames,
                              llvm::ArrayRef<mlir::Value> propValues);

    void generateCreate(const SinglePartQuery* query);
    void generateSet(const SinglePartQuery* query);
    void generateDelete(const SinglePartQuery* query);
    void generateOutput(const Projection* projection);

    // A standalone CALL ends no projection: what it yielded is the result
    void generateYieldedOutput();

    // Emits each MATCH's WHERE, each CALL and each VECTOR SEARCH of a part in the order the
    // query writes them, so a predicate or an argument reading what a statement yielded is
    // generated once that statement has bound it.
    void generateFiltersCallsAndSearches(std::span<Stmt* const> stmts);
    void generateMatchFilter(const MatchStmt* matchStmt);

    // Emits the Sort a MATCH's ORDER BY asks for, over everything in flight: the rows the
    // rest of the query reads are then the ordered ones.
    void generateMatchOrderBy(const MatchStmt* matchStmt);

    // Emits the cuts a MATCH's SKIP and LIMIT ask for, the SKIP first: what the rest of the
    // query reads is then the window rather than every row matched.
    void generateMatchWindow(const MatchStmt* matchStmt);
    void generateCall(const CallStmt* callStmt);

    void generateVectorSearch(const VectorSearchStmt* vectorSearchStmt);

    // Names the two columns a search produced as the query knows them - the yielded
    // variables of the statement, in the order it wrote them
    void publishVectorSearchYields(const YieldItems* yieldItems, mlir::Value ids, mlir::Value scores);

    // Generate the filter a CALL's YIELD ... WHERE asks for, over everything in flight once
    // the call has run - so the predicate reads the rows the procedure emitted.
    void generateYieldFilter(const YieldItems* yieldItems);

    // Fills the column of each entry of @param yielded and appends them to _yieldedColumns
    void generateCrossedCall(std::string_view procedureName,
                             llvm::ArrayRef<mlir::Attribute> yieldedNames,
                             llvm::SmallVectorImpl<YieldedColumn>& yielded,
                             mlir::ValueRange inputs,
                             const InFlightColumns& inFlight);

    // Move the dataflow built so far into the product's left factor: it is a self-contained
    // dataflow already, so every op of the block ahead of the product moves wholesale.
    void moveDataflowIntoLeftFactor(mlir::db::CrossProduct crossProduct);

    // Move the constants the left factor swallowed back out, ahead of the product.
    // collectInFlightColumns leaves a constant out of the factor's yield, so the product
    // carries no column for it and its definition has to stay where the ops after the
    // product can still read it.
    void hoistConstantsOutOfLeftFactor(mlir::db::CrossProduct crossProduct);

    // Whether a column holds the rows flowing past the current insertion point, which is
    // what an op consuming a whole row set needs of each of its operands.
    bool isRowAlignedHere(mlir::Value column) const;

    // Collect those columns. One bound in another block is skipped: an op here can only
    // take what this block binds.
    void collectInFlightColumns(InFlightColumns& inFlight);

    // Cut the rows in flight with a db.skip or a db.limit, given as @tparam CutOp
    template <typename CutOp>
    void cutAllColumns(uint64_t count);

    // Rebind them to an op's results, so later ops read what it produced. firstResult is
    // where its pass-through results start - zero for an op that only takes them (a
    // filter), past its own results for one that adds some (a call).
    void rebindInFlightColumns(mlir::Operation::result_range results,
                               size_t firstResult,
                               const InFlightColumns& inFlight);

    // The column a yielded variable holds, or a null Value when no CALL yielded it.
    mlir::Value findYieldedColumn(const VarDecl* decl) const;

    void generateWith(const WithStmt* with);

    // A barrier publishes one row per row it read, not the single row its literals are:
    // this binds a projection of constants alone to the rows of the match under it
    void broadcastConstantProjection(llvm::SmallVectorImpl<mlir::Value>& projected);

    void publishBoundColumns(const Projection* projection,
                             llvm::ArrayRef<llvm::StringRef> names,
                             llvm::ArrayRef<mlir::Value> columns);

    // Publishes every column in scope under the name it already carries, so the part that
    // follows a cut reads them the way it reads what a WITH published
    void publishInFlightColumns();

    // The column each variable in scope is bound to, under the declaration and the name
    // it carries: the traversal variables, what a CALL yielded, what a CREATE wrote, and
    // each edge identity under its own declaration. A variable a later source rebinds is
    // visited twice, with the binding that wins last, so a map filled from it holds what
    // the query reads. A standalone CALL declares nothing, so its columns are bound under
    // a null declaration.
    void forEachVariableColumn(const VariableColumnBinding& bind) const;
    void collectVariableColumns(VariableColumnMap& variableColumns) const;

    // The column one variable is bound to, over the same bindings, for a caller that reads
    // a single variable rather than every one in scope
    mlir::Value findVariableColumn(const VarDecl* decl) const;

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
    mlir::Value getOrTranslateExprColumn(const Expr* expr);

    mlir::Value resolveEntityColumn(const VarDecl* decl);

    mlir::Value nullConstantColumn();

    // Taken in the order the query declares its variables, so the choice is the query's
    // and not the addresses'
    mlir::Value resolveColumnInScope(ColumnPredicate accept) const;

    mlir::Value resolveRowCarryingColumn() const;

    // The column count(*) counts: the first variable bound to a column holding the rows
    // flowing past the insertion point. A scope of constants alone is the single row those
    // constants are, which is what count(*) counts there
    mlir::Value resolveWildcardColumn() const;

    // The column an aggregate folds over: the column count(*) counts, or the one its
    // argument's translation computes. @param variableColumns resolves an entity
    // argument, and may be null, in which case a map is built to resolve it through
    mlir::Value translateAggregateInput(const Expr* argExpr,
                                        const VariableColumnMap* variableColumns);

    void generatePropertyConstraints(std::span<Stmt* const> stmts);

    void applyPredicateFilters(std::span<const Expr* const> predicates);

    void generateGroupAggregate(const Projection* projection);

    // Publishes the grouped column of every grouping key and aggregate result an ORDER BY
    // key reads, so the key computes over one value per group rather than over the
    // ungrouped column the group aggregate consumed
    void bindOrderByKeyColumns(const Projection* projection, const GroupedColumns& groupedColumns);
    void bindGroupedKeyColumn(const Expr* expr, const GroupedColumns& groupedColumns);

    bool collectAggregateInvocations(const Expr* expr,
                                     llvm::SmallVectorImpl<const FunctionInvocationExpr*>& found);

    // One db.collect gathering every list the projection returns: the collects share the
    // group table, so a second list is another value column rather than another op
    mlir::db::Collect createCollect(llvm::ArrayRef<mlir::Value> keyColumns,
                                    llvm::ArrayRef<mlir::Value> valueColumns,
                                    llvm::ArrayRef<int64_t> distinctValues,
                                    llvm::ArrayRef<mlir::Value> aggregateColumns = {},
                                    llvm::ArrayRef<mlir::storage::GroupAggregateKind> aggregateKinds = {});

    // The collects a keyless projection returns, built as one op so a single drain emits
    // them all. One collect needs no help: its own translation is that op.
    void generateKeylessCollect(const Projection* projection);

    void translateExpr(const Expr* expr);
    void translateUnaryExpr(const Expr* expr, const UnaryExpr* unaryExpr);
    void translateBinaryExpr(const Expr* expr, const BinaryExpr* binExpr);
    void translateStringExpr(const Expr* expr);
    void translateFunctionInvocationExpr(const Expr* expr, const FunctionInvocationExpr* funcExpr);

    void translateFunctionExpr(const Expr* expr, const FunctionInvocation* invocation);

    mlir::Value translateArg(const Expr* argExpr);
    mlir::Value translateLiteralExpr(const Literal* literal);
    mlir::Value translatePropertyExpr(const PropertyExpr* propExpr);

    // The attribute carrying a scalar literal's value and type, or a null attribute for
    // any other literal kind
    mlir::TypedAttr scalarLiteralAttr(const Literal* literal);

    // The attribute carrying an embedding literal's floats: a dense f32 array, which -
    // unlike a dense tensor - stores a repeated run element by element rather than
    // uniquing it down to the one value every element holds
    mlir::Attribute embeddingLiteralAttr(const EmbeddingLiteral* literal);

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

    // Opens a dataflow from the nodes an UNWIND's literal list names, for a variable a
    // pattern uses as a node: the list seeds the traversal instead of feeding it values
    void addConstScanNodes(const VariableDependency* var, const UnwindStmt* unwind);

    void filterAllColumns(mlir::Value predicate);

    // Such a scope is the single row those constants are: the predicate and the columns
    // are laid out over that row
    void filterConstantScope(mlir::Value predicate);

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
    void rebindYieldedColumn(const VarDecl* decl, mlir::TypedValue<mlir::Type> val);
};

}
