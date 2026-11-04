#pragma once

#include <string_view>
#include <unordered_set>
#include <vector>

#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
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
class RemoveStmt;
class DeleteStmt;
class Pattern;
class PatternElement;
class EntityPattern;
class NodePattern;
class EdgePattern;
class Expr;
class MapLiteral;

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
    void analyzePropertyValues(const MapLiteral* properties, const EntityPattern* pattern);
    void analyze(SetItem* item);
    void analyzePropertiesAssign(SetItem::EntityPropertiesAssign& assign, const SetItem* item);
    void analyzeMapLiteralProperties(const Expr* value, std::vector<EntityPropertyConstraint>& created);

    void throwOnEntityWhere(const Pattern* pattern, std::string_view clause) const;
    void throwOnUndirectedEdge(const Pattern* pattern) const;
    void throwOnNullProperty(const Pattern* pattern) const;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;

    static ValueType evaluatedToValueType(EvaluatedType type);
};

}
