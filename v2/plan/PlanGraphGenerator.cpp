#include "PlanGraphGenerator.h"

#include "views/GraphView.h"

#include "CypherAST.h"
#include "PlanGraph.h"
#include "PlanGraphTopology.h"
#include "ReadStatementGenerator.h"
#include "WriteStatementGenerator.h"

#include "nodes/JoinNode.h"
#include "nodes/CartesianProductNode.h"
#include "nodes/ProduceResultsNode.h"

#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "stmt/StmtContainer.h"

#include "decl/VarDecl.h"
#include "decl/PatternData.h"

#include "PlannerException.h"

using namespace db::v2;
using namespace db;

PlanGraphGenerator::PlanGraphGenerator(const CypherAST& ast,
                                       const GraphView& view)
    : _ast(&ast),
      _view(view),
      _variables(&_tree) {
}

PlanGraphGenerator::~PlanGraphGenerator() {
}

void PlanGraphGenerator::generate(const QueryCommand* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            generateSinglePartQuery(static_cast<const SinglePartQuery*>(query));
            break;

        default:
            throwError(fmt::format("Unsupported query command of type {}", (uint64_t)query->getKind()), query);
            break;
    }
}

void PlanGraphGenerator::generateSinglePartQuery(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    const StmtContainer* updateStmts = query->getUpdateStmts();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    // Generate read statements (optional)
    if (readStmts) {
        ReadStatementGenerator readGenerator(_ast, _view, &_tree, &_variables);

        for (const Stmt* stmt : readStmts->stmts()) {
            readGenerator.generateStmt(stmt);
        }

        // Place joins on vars that have more than one input
        readGenerator.placeJoinsOnVars();

        // Place joins based on property expressions
        readGenerator.placePropertyExprJoins();

        // Place joins based on where predicates
        readGenerator.placePredicateJoins();
    }

    // Generate update statements (optional)
    if (updateStmts) {
        WriteStatementGenerator writeGenerator(_ast, &_tree, &_variables);

        for (const Stmt* stmt : updateStmts->stmts()) {
            writeGenerator.generateStmt(stmt);
        }
    }

    // Generate return statement
    if (returnStmt) {
        generateReturnStmt(returnStmt);
    }
}

void PlanGraphGenerator::generateReturnStmt(const ReturnStmt* stmt) {
    // Step 1: Find all end points
    std::vector<PlanGraphNode*> ends;
    for (const auto& node : _tree.nodes()) {
        if (node->outputs().empty()) {
            ends.push_back(node.get());
        }
    }

    if (ends.empty()) {
        /* Right now (a)-->(b)-->(c)-->(a) is a loop, which means that we
         * cannot define an endpoint.
         *
         * This needs to be explictely handled,
         * probably using "loop unrolling". When we detect a loop, we actually
         * define a new variable (a') in this example, and add a constraint,
         * WHERE a == a'.
         *
         * To implement this, we need to:
         *
         * - Allow comparing entities (e.g. a == b) and test the query:
         *   `MATCH (a)-->(b) WHERE a == b RETURN *`
         * - Then, add the unrolling logic to the query planner. This may
         *   be as simple as: in planOrigin and planTarget, if the
         *   node already exists, detect if we can come back to the same
         *   position by going backwards. If so, create a new unnamed variable
         *   and add the constraint.
         * */

        throwError("No endpoints found, loops are not supported yet", stmt);
    }

    if (ends.size() == 1) {
        // No joins needed, just generate the last ProduceResultsNode
        ProduceResultsNode* results = _tree.create<ProduceResultsNode>();
        ends.back()->connectOut(results);
        return;
    }

    // Step 2: Generate all joins
    // Algorithm:
    // - Pick the first endpoint (= rhs)
    // - For each other endpoint (= lhs):
    //     - Find the shortest path between the lhs and rhs
    //     - If the path is undirected, JOIN the two endpoints
    //     - If no path is found, CARTESIAN_PRODUCT the two endpoints
    //     - rhs becomes the join node
    /*       A              A              A
     *      / \            / \            / \
     *     B   C          B   C          B   C
     *    /     \    ->  /     \    ->  /     \
     *   D       F      D       F      D       F
     *    \     / \      \     / \      \     / \
     *     H   I   J      H   I   J      H   I   J
     *                         \ /        \   \ /
     *                         [u]         \  [u]
     *                                      \ /
     *                                      [u]   */

    PlanGraphNode* rhsNode = ends[0];

    for (size_t i = 1; i < ends.size(); i++) {
        PlanGraphNode* lhsNode = ends[i];

        const auto path = PlanGraphTopology::getShortestPath(rhsNode, lhsNode);

        switch (path) {
            case PlanGraphTopology::PathToDependency::SameVar:
            case PlanGraphTopology::PathToDependency::BackwardPath: {
                // Should not happen
                throwError("Unknown error", rhsNode);
                continue;
            } break;

            case PlanGraphTopology::PathToDependency::UndirectedPath: {
                // Join
                JoinNode* join = _tree.create<JoinNode>();
                rhsNode->connectOut(join);
                lhsNode->connectOut(join);

                rhsNode = join;
            } break;
            case PlanGraphTopology::PathToDependency::NoPath: {
                // Cartesian product
                CartesianProductNode* join = _tree.create<CartesianProductNode>();
                rhsNode->connectOut(join);
                lhsNode->connectOut(join);

                rhsNode = join;
            } break;
        }
    }

    // Step 3: Connect the last node to the output
    ProduceResultsNode* results = _tree.create<ProduceResultsNode>();
    rhsNode->connectOut(results);
}

void PlanGraphGenerator::throwError(std::string_view msg, const void* obj) const {
    throw PlannerException(_ast->createErrorString(msg, obj));
}
