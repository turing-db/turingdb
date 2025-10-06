#pragma once

#include <string_view>

namespace db::v2 {

class CypherAST;
class PlanGraph;
class PlanGraphNode;
class PlanGraphVariables;
class Stmt;
class CreateStmt;
class PatternElement;
class Expr;
class VarNode;
class NodePattern;
class EdgePattern;

class WriteStatementGenerator {
public:
    WriteStatementGenerator(const CypherAST* ast,
                           PlanGraph* tree,
                           PlanGraphVariables* variables);

    ~WriteStatementGenerator();

    void generateStmt(const Stmt* stmt);
    void generateCreateStmt(const CreateStmt* stmt);
    void generatePatternElement(const PatternElement* element);

    VarNode* generatePatternElementOrigin(const NodePattern* origin);
    VarNode* generatePatternElementEdge(VarNode* prevNode, const EdgePattern* edge);
    VarNode* generatePatternElementTarget(VarNode* prevNode, const NodePattern* target);

private:
    const CypherAST* _ast {nullptr};
    PlanGraph* _tree {nullptr};
    PlanGraphVariables* _variables {nullptr};

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
