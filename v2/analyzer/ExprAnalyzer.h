#pragma once

#include <string_view>
#include <unordered_map>
#include <vector>

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

class ExprAnalyzer {
public:
    ExprAnalyzer(const CypherAST* ast, const GraphView& graphView);
    ~ExprAnalyzer();

    ExprAnalyzer(const ExprAnalyzer&) = delete;
    ExprAnalyzer(ExprAnalyzer&&) = delete;
    ExprAnalyzer& operator=(const ExprAnalyzer&) = delete;
    ExprAnalyzer& operator=(ExprAnalyzer&&) = delete;

    void setDeclContext(DeclContext* ctxt) { _ctxt = ctxt; }

    // Expressions
    void analyze(Expr* expr);
    void analyze(BinaryExpr* expr);
    void analyze(UnaryExpr* expr);
    void analyze(SymbolExpr* expr);
    void analyze(LiteralExpr* expr);
    void analyze(PropertyExpr* expr);
    void analyze(StringExpr* expr);
    void analyze(EntityTypeExpr* expr);
    void analyze(PathExpr* expr);

    static bool propTypeCompatible(ValueType vt, EvaluatedType exprType);

private:
    const CypherAST* _ast {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    const GraphMetadata& _graphMetadata;

    std::unordered_map<std::string_view, ValueType> _typeMap;
    std::vector<std::string_view> _toBeCreated;

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
