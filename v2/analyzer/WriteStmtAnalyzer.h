#pragma once

#include <string_view>
#include <unordered_set>

#include "decl/EvaluatedType.h"
#include "views/GraphView.h"

namespace db::v2 {

class CypherAST;
class VarDecl;
class DeclContext;
class ExprAnalyzer;
class Stmt;
class CreateStmt;
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
    void analyze(const CreateStmt* createStmt);
    void analyze(const DeleteStmt* deleteStmt);
    void analyze(const Pattern* pattern);
    void analyze(const PatternElement* element);
    void analyze(NodePattern* node);
    void analyze(EdgePattern* edge);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    ExprAnalyzer* _exprAnalyzer {nullptr};
    std::unordered_set<const VarDecl*> _toBeCreated;
    const GraphMetadata& _graphMetadata;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;

    static ValueType evaluatedToValueType(EvaluatedType type);
};

}
