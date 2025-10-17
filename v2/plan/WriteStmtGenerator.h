#pragma once

#include <string_view>

namespace db::v2 {

class CypherAST;
class WriteNode;
class VarDecl;
class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
class Stmt;
class CreateStmt;
class DeleteStmt;
class PatternElement;
class Expr;
class VarNode;
class NodePattern;
class EdgePattern;

class WriteStmtGenerator {
public:
    WriteStmtGenerator(const CypherAST* ast,
                       PlanGraph* tree,
                       PlanGraphVariables* variables);

    ~WriteStmtGenerator();

    WriteStmtGenerator(const WriteStmtGenerator&) = delete;
    WriteStmtGenerator(WriteStmtGenerator&&) = delete;
    WriteStmtGenerator& operator=(const WriteStmtGenerator&) = delete;
    WriteStmtGenerator& operator=(WriteStmtGenerator&&) = delete;

    WriteNode* generateStmt(const Stmt* stmt, PlanGraphNode* prevNode = nullptr);
    void generateCreateStmt(const CreateStmt* stmt, PlanGraphNode* prevNode);
    void generateDeleteStmt(const DeleteStmt* stmt, PlanGraphNode* prevNode);
    void generatePatternElement(const PatternElement* element);

    void generatePatternElementNode(NodePattern* origin);
    void generatePatternElementEdge(const NodePattern* src,
                                    EdgePattern* edge,
                                    const NodePattern* dst);

private:
    const CypherAST* _ast {nullptr};
    PlanGraph* _tree {nullptr};
    PlanGraphVariables* _variables {nullptr};
    WriteNode* _currentNode {nullptr};

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
