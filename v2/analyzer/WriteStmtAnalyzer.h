#pragma once

#include <string_view>

#include "decl/EvaluatedType.h"
#include "views/GraphView.h"

namespace db::v2 {

class CypherAST;
class AnalyzerVariables;
class ExprAnalyzer;
class Stmt;
class CreateStmt;
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

    void setVariables(AnalyzerVariables* variables) {
        _variables = variables;
    }

    void setExprAnalyzer(ExprAnalyzer* exprAnalyzer) {
        _exprAnalyzer = exprAnalyzer;
    }

    // Statements
    void analyze(const Stmt* stmt);
    void analyze(const CreateStmt* createStmt);
    void analyze(const Pattern* pattern);
    void analyze(const PatternElement* element);
    void analyze(NodePattern* node);
    void analyze(EdgePattern* edge);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    AnalyzerVariables* _variables {nullptr};
    ExprAnalyzer* _exprAnalyzer {nullptr};
    const GraphMetadata& _graphMetadata;

    std::vector<std::pair<NodeID, std::string_view>> _additionalNodeLabels;

    void throwError(std::string_view msg, const void* obj = 0) const;

    static ValueType evaluatedToValueType(EvaluatedType type);
};

}
