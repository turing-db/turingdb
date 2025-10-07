#pragma once

#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "EntityProperties.h"

namespace db::v2 {

class CypherAST;
class VarDecl;
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

    WriteStatementGenerator(const WriteStatementGenerator&) = delete;
    WriteStatementGenerator(WriteStatementGenerator&&) = delete;
    WriteStatementGenerator& operator=(const WriteStatementGenerator&) = delete;
    WriteStatementGenerator& operator=(WriteStatementGenerator&&) = delete;

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

    std::unordered_map<const VarDecl*, VarNode*> _nodeMap;
    std::unordered_map<const VarDecl*, EntityProperties> _entityPropsMap;
    std::unordered_set<const VarNode*> _inputs;

    uint32_t _nextDeclOrder {0};

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
