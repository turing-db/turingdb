#include "WriteStatementGenerator.h"

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "PlanGraph.h"
#include "WhereClause.h"
#include "PlanGraphVariables.h"

#include "decl/VarDecl.h"
#include "nodes/FilterNode.h"
#include "nodes/VarNode.h"

#include "stmt/Stmt.h"
#include "stmt/CreateStmt.h"

#include "decl/PatternData.h"

using namespace db::v2;

WriteStatementGenerator::WriteStatementGenerator(const CypherAST* ast,
                                                 PlanGraph* tree,
                                                 PlanGraphVariables* variables)
    : _ast(ast),
      _tree(tree),
      _variables(variables) {
}

WriteStatementGenerator::~WriteStatementGenerator() {
}

void WriteStatementGenerator::generateStmt(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::CREATE:
            generateCreateStmt(static_cast<const CreateStmt*>(stmt));
            break;

        default:
            throwError(fmt::format("Unsupported write statement type: {}", (uint64_t)stmt->getKind()), stmt);
            break;
    }
}

void WriteStatementGenerator::generateCreateStmt(const CreateStmt* stmt) {
    const Pattern* pattern = stmt->getPattern();

    // Each PatternElement is a target of the create
    // and contains a chain of EntityPatterns
    for (const PatternElement* element : pattern->elements()) {
        generatePatternElement(element);
    }
}

void WriteStatementGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throwError("Empty match pattern element", element);
    }

    const NodePattern* origin = dynamic_cast<const NodePattern*>(element->getRootEntity());
    if (!origin) {
        throwError("Pattern element origin must be a node pattern", element);
    }

    // Reset the decl order of variables (each branch starts at 0)
    _variables->resetDeclOrder();

    VarNode* currentNode = generatePatternElementOrigin(origin);

    const auto& chain = element->getElementChain();
    for (const auto& [edge, node] : chain) {
        const EdgePattern* e = dynamic_cast<const EdgePattern*>(edge);
        if (!edge) {
            throwError("Pattern element edge must be an edge pattern", element);
        }

        currentNode = generatePatternElementEdge(currentNode, e);

        const NodePattern* n = dynamic_cast<const NodePattern*>(node);
        if (!node) {
            throwError("Pattern element node must be a node pattern", element);
        }

        currentNode = generatePatternElementTarget(currentNode, n);
    }
}

VarNode* WriteStatementGenerator::generatePatternElementOrigin(const NodePattern* origin) {
    // const VarDecl* decl = origin->getDecl();
    // VarNode* var = _variables->getVarNode(decl);

    // if (!var) {
    //     // Create a new node
    //     var = _tree->create<VarNode>(decl, _nextDeclOrder++);
    //     _nodeMap.insert({decl, var});
    // } else {
    //     // The node is an input to the write query
    //     _inputs.insert(var);
    // }

    // return var;
    return nullptr;
}

VarNode* WriteStatementGenerator::generatePatternElementEdge(VarNode* prevNode,
                                                             const EdgePattern* edge) {
    // const VarDecl* decl = edge->getDecl();
    // VarNode* var = _variables->getVarNode(decl);

    // if (var) {
    //     throwError(fmt::format("Edge {} already declared", decl->getName()), edge);
    // }

    // var = _tree->create<VarNode>(decl, _nextDeclOrder++);
    // _nodeMap.insert({decl, var});

    // prevNode->connectOut(var);

    // return var;
    return nullptr;
}

VarNode* WriteStatementGenerator::generatePatternElementTarget(VarNode* prevNode,
                                                               const NodePattern* target) {
    // const VarDecl* decl = target->getDecl();
    // VarNode* var = _variables->getVarNode(decl);

    // if (!var) {
    //     // Create a new node
    //     var = _tree->create<VarNode>(decl, _nextDeclOrder++);
    //     _nodeMap.insert({decl, var});
    // } else {
    //     // The node is an input to the write query
    //     _inputs.insert(var);
    // }

    // prevNode->connectOut(var);

    // return var;
    return nullptr;
}

void WriteStatementGenerator::throwError(std::string_view msg, const void* obj) const {
    throw PlannerException(_ast->createErrorString(msg, obj));
}
