#pragma once

#include <string_view>

namespace db {

class CypherAST;
class WriteNode;
class VarDecl;
class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
class Stmt;
class SetStmt;
class CreateStmt;
class DeleteStmt;
class PatternElement;
class Expr;
class VarNode;
class NodePattern;
class EdgePattern;
class GetPropertyCache;
class PropertyExpr;

class WriteStmtGenerator {
public:
    WriteStmtGenerator(const CypherAST* ast,
                       PlanGraph* tree,
                       PlanGraphVariables* variables,
                       PlanGraphNode* prevNode);

    ~WriteStmtGenerator();

    WriteStmtGenerator(const WriteStmtGenerator&) = delete;
    WriteStmtGenerator(WriteStmtGenerator&&) = delete;
    WriteStmtGenerator& operator=(const WriteStmtGenerator&) = delete;
    WriteStmtGenerator& operator=(WriteStmtGenerator&&) = delete;

    WriteNode* generateStmt(const Stmt* stmt);
    void generateSetStmt(const SetStmt* stmt);
    void generateCreateStmt(const CreateStmt* stmt);
    void generateDeleteStmt(const DeleteStmt* stmt);
    void generateCreatePatternElement(const PatternElement* element);

    void generateCreatePatternElementNode(NodePattern* origin);
    void generateCreatePatternElementEdge(const NodePattern* src,
                                          EdgePattern* edge,
                                          const NodePattern* dst);
    void setPrevNode(PlanGraphNode* node) { _prevNode = node; }

private:
    const CypherAST* _ast {nullptr};
    PlanGraph* _tree {nullptr};
    PlanGraphVariables* _variables {nullptr};
    WriteNode* _writeNode {nullptr};
    PlanGraphNode* _prevNode {nullptr};

    GetPropertyCache& _propCache;

    void prepareWriteNode();

    void fetchOrGenerateProperty(PropertyExpr* prop);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;

    static constexpr bool DELETE_PENDING_SUPPORTED {false};
};

}
