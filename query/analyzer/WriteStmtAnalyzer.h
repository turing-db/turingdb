#pragma once

#include <string_view>
#include <unordered_set>

#include "decl/EvaluatedType.h"
#include "stmt/SetItem.h"
#include "views/GraphView.h"

namespace db {

class CypherAST;
class VarDecl;
class DeclContext;
class ExprAnalyzer;
class Stmt;
class CreateStmt;
class MergeStmt;
class SetStmt;
class PropertyExpr;
class Expr;
class RemoveStmt;
class DeleteStmt;
class Pattern;
class PatternElement;
class NodePattern;
class EdgePattern;

class WriteStmtAnalyzer {
public:
    explicit WriteStmtAnalyzer(CypherAST* ast, GraphView graphView);
    ~WriteStmtAnalyzer();

    WriteStmtAnalyzer(const WriteStmtAnalyzer&) = delete;
    WriteStmtAnalyzer(WriteStmtAnalyzer&&) = delete;
    WriteStmtAnalyzer& operator=(const WriteStmtAnalyzer&) = delete;
    WriteStmtAnalyzer& operator=(WriteStmtAnalyzer&&) = delete;

    void setDeclContext(DeclContext* ctxt) {
        _ctxt = ctxt;
    }

    void setExprAnalyzer(ExprAnalyzer* exprAnalyzer) {
        _exprAnalyzer = exprAnalyzer;
    }

    // Statements
    void analyze(const Stmt* stmt);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    ExprAnalyzer* _exprAnalyzer {nullptr};
    std::unordered_set<const VarDecl*> _toBeCreated;
    const GraphMetadata& _graphMetadata;

    // The variables the CREATE or MERGE being analyzed introduces, which its property maps
    // cannot read: none has a value until the clause has run
    std::unordered_set<std::string_view> _patternNames;
    std::string_view _patternClause;

    void analyze(const CreateStmt* createStmt);
    void analyze(const MergeStmt* mergeStmt);
    void analyze(const SetStmt* setStmt);
    void analyze(const RemoveStmt* removeStmt);
    void analyze(const DeleteStmt* deleteStmt);
    void analyze(const Pattern* pattern);
    void analyze(PatternElement* element);
    void analyzeNamedPath(PatternElement* element);
    void analyze(NodePattern* node);
    void analyze(EdgePattern* edge);
    void analyze(SetItem* item);
    void analyzePropertyAssign(const SetItem* item, PropertyExpr* lhs, Expr* rhs);
    void analyzeMapAssign(const SetItem* item, SetItem::SymbolMapAssign& assign);
    void analyzeComputedValue(const SetItem* item, SetItem::SymbolMapAssign& assign);
    void analyzeEntityCopy(const SetItem* item, SetItem::SymbolMapAssign& assign);

    void throwOnEntityWhere(const Pattern* pattern, std::string_view clause) const;
    void throwOnUndirectedEdge(const Pattern* pattern) const;
    void collectPatternNames(const Pattern* pattern);
    void throwOnPatternEntityRead(const Expr* expr, const void* obj) const;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;

    static ValueType evaluatedToValueType(EvaluatedType type);
};

}
