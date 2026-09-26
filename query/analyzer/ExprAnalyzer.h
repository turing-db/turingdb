#pragma once

#include <span>
#include <vector>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "decl/EvaluatedType.h"
#include "expr/CaseExpr.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"

namespace db {
class GraphMetadata;
}

namespace db {

class CypherAST;
class CypherAnalyzer;
class DeclContext;
class FunctionResolver;
class LoadCSVStmt;
class VarDecl;
class Expr;
class BinaryExpr;
class UnaryExpr;
class SymbolExpr;
class LiteralExpr;
class PropertyExpr;
class PropertyLookupExpr;
class StringExpr;
class EntityTypeExpr;
class FunctionInvocationExpr;
class IndexExpr;
class Symbol;
class NodePattern;
class EdgePattern;
class ListExpr;
class ListComprehensionExpr;
class ExistsExpr;
class ListSliceExpr;
class MapLiteral;
class PatternComprehensionExpr;
class ReadStmtAnalyzer;

class ExprAnalyzer {
public:
    ExprAnalyzer(CypherAST* ast, const GraphView& graphView);
    ~ExprAnalyzer();

    ExprAnalyzer(const ExprAnalyzer&) = delete;
    ExprAnalyzer(ExprAnalyzer&&) = delete;
    ExprAnalyzer& operator=(const ExprAnalyzer&) = delete;
    ExprAnalyzer& operator=(ExprAnalyzer&&) = delete;

    void setDeclContext(DeclContext* ctxt) { _ctxt = ctxt; }

    // The analyzer an EXISTS body is analyzed by: its statements are a query of their own,
    // which only the query analyzer knows how to walk
    void setQueryAnalyzer(CypherAnalyzer* analyzer) { _queryAnalyzer = analyzer; }

    // The analyzer of the MATCH a pattern comprehension holds its pattern as, which
    // declares the variables that pattern binds
    void setReadAnalyzer(ReadStmtAnalyzer* readAnalyzer) { _readAnalyzer = readAnalyzer; }

    // Collects, while set, every variable an expression reads from a scope enclosing the
    // current one. A hop of a quantified pattern analyses its predicate in a scope of its
    // own, and what that predicate reaches out of it is what the exploration must hand it
    void setImportSink(std::vector<const VarDecl*>* sink) { _importSink = sink; }

    // Declares the statement a CSV row variable is loaded by, so `row[2]` and `row.age`
    // resolve to fields of that statement rather than to accesses of their own
    void registerCSVSource(const VarDecl* alias, LoadCSVStmt* loadCSV);

    // Expressions
    void analyzeRootExpr(Expr* expr);
    void analyzeExpr(Expr* expr);
    void analyzeBinaryExpr(BinaryExpr* expr);
    void analyzeUnaryExpr(UnaryExpr* expr);
    void analyzeSymbolExpr(SymbolExpr* expr);
    void analyzeLiteralExpr(LiteralExpr* expr);
    void analyzeListExpr(ListExpr* expr);
    void analyzeListComprehensionExpr(ListComprehensionExpr* expr);
    void analyzePatternComprehensionExpr(PatternComprehensionExpr* expr);
    void analyzeCaseExpr(CaseExpr* expr);
    void analyzeExistsExpr(ExistsExpr* expr);
    void analyzeStringExpr(StringExpr* expr);
    void analyzeEntityTypeExpr(EntityTypeExpr* expr);
    void analyzeFuncInvocExpr(FunctionInvocationExpr* expr, FunctionResolver* resolver);
    void analyzeIndexExpr(IndexExpr* expr);
    void analyzeListSliceExpr(ListSliceExpr* expr);
    void analyzePropertyLookupExpr(PropertyLookupExpr* expr);

    ValueType analyzePropertyExpr(PropertyExpr* expr,
                                  bool allowCreate = false,
                                  ValueType defaultType = ValueType::Invalid);

    // A write and an index name a property of the graph; a calendar field read off one is
    // an integer computed from it, which names nothing that can be written or indexed.
    void throwIfReadsADateTimeComponent(const PropertyExpr* expr);

    void addToBeCreatedType(std::string_view name, ValueType type, const void* obj = nullptr);

    static bool propTypeCompatible(ValueType vt, EvaluatedType exprType);

    /// Adds an empty declaration for the given NodePattern
    void registerNodePatternDeclaration(const NodePattern* node);

    /// Adds an empty declaration for the given EdgePattern
    void registerEdgePatternDeclaration(const EdgePattern* edge);

private:
    CypherAST* _ast {nullptr};
    CypherAnalyzer* _queryAnalyzer {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    ReadStmtAnalyzer* _readAnalyzer {nullptr};
    const GraphMetadata& _graphMetadata;

    std::unordered_map<std::string_view, ValueType> _toBeCreatedTypes;

    // The LOAD CSV each row variable in scope was bound by
    std::unordered_map<const VarDecl*, LoadCSVStmt*> _csvSources;

    std::unordered_set<const Expr*> _analyzedExprs;
    std::vector<const VarDecl*>* _importSink {nullptr};

    // The declaration a name reads as, noting it when it came from an enclosing scope
    VarDecl* resolveVariable(std::string_view name);

    void analyzeListElements(Expr* expr, std::span<Expr* const> elements);
    void analyzeMapEntries(Expr* expr, const MapLiteral* map);

    // The type a CASE has once @param branch is folded into the type its earlier branches
    // already share, reporting a pair no column type can hold
    EvaluatedType unifyCaseBranch(EvaluatedType carried, const Expr* branch);

    // Rejects a CASE subject no column holds. A node and an edge are subjects of their
    // own: the MLIR engine binds one, and an OPTIONAL MATCH can leave it null
    void requireCaseSubject(const Expr* subject) const;

    // Rejects a value a CASE compares its scalar subject against that no column holds
    void requireCaseValue(const Expr* value) const;

    // Rejects a @param test the entity @param subject does not compare against: an
    // ordering branch, or a value that is neither an entity of its kind nor an id
    void requireComparableToEntity(const Expr* subject, const CaseExpr::Test& test) const;

    // One WHEN value of @param branch, read as a predicate when the CASE has no subject
    // and as something to compare that subject against when it has one
    void analyzeCaseTest(const CaseExpr::Branch& branch, const Expr* subject, const CaseExpr::Test& test);

    // The type the arguments of a call to @param name share, folded the way a CASE folds
    // its branches, reporting a pair no column type can hold
    EvaluatedType unifiedArgumentType(std::string_view name, std::span<Expr* const> args) const;

    LoadCSVStmt* findCSVSource(const VarDecl* alias) const;

    // The declaration the load publishes field @param slot under, created by the first
    // access to reach that field so every later one resolves to the same column
    VarDecl* declareCSVField(LoadCSVStmt& loadCSV, size_t slot);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
