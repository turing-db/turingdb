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
class Symbol;

class ExprAnalyzer {
public:
    ExprAnalyzer(CypherAST* ast, const GraphView& graphView);
    ~ExprAnalyzer();

    ExprAnalyzer(const ExprAnalyzer&) = delete;
    ExprAnalyzer(ExprAnalyzer&&) = delete;
    ExprAnalyzer& operator=(const ExprAnalyzer&) = delete;
    ExprAnalyzer& operator=(ExprAnalyzer&&) = delete;

    void setDeclContext(DeclContext* ctxt) { _ctxt = ctxt; }

    // Expressions
    void analyzeRootExpr(Expr* expr);
    void analyzeExpr(Expr* expr);
    void analyzeBinaryExpr(BinaryExpr* expr);
    void analyzeUnaryExpr(UnaryExpr* expr);
    void analyzeSymbolExpr(SymbolExpr* expr);
    void analyzeLiteralExpr(LiteralExpr* expr);
    void analyzeStringExpr(StringExpr* expr);
    void analyzeEntityTypeExpr(EntityTypeExpr* expr);
    void analyzeFuncInvocExpr(FunctionInvocationExpr* expr);
    void analyzePathExpr(PathExpr* expr);

    ValueType analyzePropertyExpr(PropertyExpr* expr,
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

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
