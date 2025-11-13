#pragma once

#include <string_view>
#include <unordered_map>

#include "decl/EvaluatedType.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"

namespace db {
class GraphMetadata;
}

namespace db::v2 {

class CypherAST;
class DeclContext;
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

class ExprAnalyzer {
public:
    ExprAnalyzer(CypherAST* ast, const GraphView& graphView);
    ~ExprAnalyzer();

    ExprAnalyzer(const ExprAnalyzer&) = delete;
    ExprAnalyzer(ExprAnalyzer&&) = delete;
    ExprAnalyzer& operator=(const ExprAnalyzer&) = delete;
    ExprAnalyzer& operator=(ExprAnalyzer&&) = delete;

    void setDeclContext(DeclContext* ctxt) { _ctxt = ctxt; }

    void analyzeRootExpr(Expr* expr);

    ValueType analyze(PropertyExpr* expr,
                      bool allowCreate = false,
                      ValueType defaultType = ValueType::Invalid);

    void addToBeCreatedType(std::string_view name, ValueType type, const void* obj = nullptr);

    static bool propTypeCompatible(ValueType vt, EvaluatedType exprType);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    const GraphMetadata& _graphMetadata;

    std::unordered_map<std::string_view, ValueType> _toBeCreatedTypes;

    // Expressions
    void analyze(Expr* expr);
    void analyze(BinaryExpr* expr);
    void analyze(UnaryExpr* expr);
    void analyze(SymbolExpr* expr);
    void analyze(LiteralExpr* expr);
    void analyze(StringExpr* expr);
    void analyze(EntityTypeExpr* expr);
    void analyze(PathExpr* expr);
    void analyze(FunctionInvocationExpr* expr);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
