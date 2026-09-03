#pragma once

#include <span>
#include <string_view>
#include <unordered_map>

#include "decl/EvaluatedType.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"

namespace db {
class GraphMetadata;
}

namespace db {

class CypherAST;
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
class StringExpr;
class EntityTypeExpr;
class PathExpr;
class FunctionInvocationExpr;
class IndexExpr;
class Symbol;
class NodePattern;
class EdgePattern;
class ListExpr;
class MapLiteral;

class ExprAnalyzer {
public:
    ExprAnalyzer(CypherAST* ast, const GraphView& graphView);
    ~ExprAnalyzer();

    ExprAnalyzer(const ExprAnalyzer&) = delete;
    ExprAnalyzer(ExprAnalyzer&&) = delete;
    ExprAnalyzer& operator=(const ExprAnalyzer&) = delete;
    ExprAnalyzer& operator=(ExprAnalyzer&&) = delete;

    void setDeclContext(DeclContext* ctxt) { _ctxt = ctxt; }
    void setV3() { _isV3 = true; }

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
    void analyzeStringExpr(StringExpr* expr);
    void analyzeEntityTypeExpr(EntityTypeExpr* expr);
    void analyzeFuncInvocExpr(FunctionInvocationExpr* expr, FunctionResolver* resolver);
    void analyzePathExpr(PathExpr* expr);
    void analyzeIndexExpr(IndexExpr* expr);

    ValueType analyzePropertyExpr(PropertyExpr* expr,
                                  bool allowCreate = false,
                                  ValueType defaultType = ValueType::Invalid);

    void addToBeCreatedType(std::string_view name, ValueType type, const void* obj = nullptr);

    static bool propTypeCompatible(ValueType vt, EvaluatedType exprType);

    /// Adds an empty declaration for the given NodePattern
    void registerNodePatternDeclaration(const NodePattern* node);

    /// Adds an empty declaration for the given EdgePattern
    void registerEdgePatternDeclaration(const EdgePattern* edge);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    const GraphMetadata& _graphMetadata;
    bool _isV3 {false};

    std::unordered_map<std::string_view, ValueType> _toBeCreatedTypes;

    // The LOAD CSV each row variable in scope was bound by
    std::unordered_map<const VarDecl*, LoadCSVStmt*> _csvSources;

    void analyzeListElements(Expr* expr, std::span<Expr* const> elements);
    void analyzeMapEntries(Expr* expr, const MapLiteral* map);

    LoadCSVStmt* findCSVSource(const VarDecl* alias) const;

    // The declaration the load publishes field @param slot under, created by the first
    // access to reach that field so every later one resolves to the same column
    VarDecl* declareCSVField(LoadCSVStmt& loadCSV, size_t slot);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
