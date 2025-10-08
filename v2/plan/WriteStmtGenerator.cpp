#include "WriteStmtGenerator.h"

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

#include "nodes/WriteNode.h"
#include "stmt/Stmt.h"
#include "stmt/CreateStmt.h"

#include "decl/PatternData.h"

using namespace db::v2;

WriteStmtGenerator::WriteStmtGenerator(const CypherAST* ast,
                                       PlanGraph* tree,
                                       PlanGraphVariables* variables)
    : _ast(ast),
    _tree(tree),
    _variables(variables)
{
}

WriteStmtGenerator::~WriteStmtGenerator() {
}

WriteNode* WriteStmtGenerator::generateStmt(const Stmt* stmt, PlanGraphNode* prevNode) {
    switch (stmt->getKind()) {
        case Stmt::Kind::CREATE:
            generateCreateStmt(static_cast<const CreateStmt*>(stmt), prevNode);
            break;

        default:
            throwError(fmt::format("Unsupported write statement type: {}", (uint64_t)stmt->getKind()), stmt);
            break;
    }

    return _currentNode;
}

void WriteStmtGenerator::generateCreateStmt(const CreateStmt* stmt, PlanGraphNode* prevNode) {
    _currentNode = prevNode
                     ? _tree->newOut<WriteNode>(prevNode)
                     : _tree->create<WriteNode>();

    const Pattern* pattern = stmt->getPattern();

    for (const PatternElement* element : pattern->elements()) {
        generatePatternElement(element);
    }
}

void WriteStmtGenerator::generatePatternElement(const PatternElement* element) {
    NodePattern* origin = dynamic_cast<NodePattern*>(element->getRootEntity());

    // Step 1. Handle the origin

    if (!origin) [[unlikely]] {
        throwError("CREATE statement must have a node pattern as origin", element);
    }

    const VarDecl* originDecl = origin->getDecl();
    const NodePatternData* data = origin->getData();

    WriteNode::EdgeNeighbour lhs;
    WriteNode::EdgeNeighbour rhs;

    if (VarNode* var = _variables->getVarNode(originDecl)) {
        // Node already exists, this is an input to the write query
        // The execution will have to use the columns corresponding to it
        _inputs.insert(var);
        lhs.emplace<const VarDecl*>(originDecl);
    } else {
        // Create a new node
        const size_t offset = _currentNode->addNode(data);
        lhs.emplace<size_t>(offset);
    }

    // Step 2. Handle the [edge-rhs] chains

    for (auto [edge, rhsNode] : element->getElementChain()) {
        // - Get/Create the rhs node
        const VarDecl* rhsDecl = rhsNode->getDecl();
        const NodePatternData* rhsData = rhsNode->getData();

        if (VarNode* var = _variables->getVarNode(rhsDecl)) {
            _inputs.insert(var);
            rhs.emplace<const VarDecl*>(rhsDecl);
        } else {
            const size_t rhsOffset = _currentNode->addNode(rhsData);
            rhs.emplace<size_t>(rhsOffset);
        }

        // - Create the new edge
        switch (edge->getDirection()) {
            case EdgePattern::Direction::Undirected:
                throwError("Cannot use undirected edges in write statements", edge);
            case EdgePattern::Direction::Backward:
                _currentNode->addEdge(rhs, edge->getData(), lhs);
                break;
            case EdgePattern::Direction::Forward:
                _currentNode->addEdge(lhs, edge->getData(), rhs);
                break;
        }

        lhs = rhs;
    }
}

void WriteStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    throw PlannerException(_ast->createErrorString(msg, obj));
}
