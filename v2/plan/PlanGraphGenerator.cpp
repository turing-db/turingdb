#include "PlanGraphGenerator.h"

#include "views/GraphView.h"

#include "CypherAST.h"
#include "PlanGraph.h"
#include "ReadStmtGenerator.h"
#include "WriteStmtGenerator.h"

#include "nodes/WriteNode.h"
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
    _variables(&_tree)
{
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

    PlanGraphNode* currentNode = nullptr;

    // Generate read statements (optional)
    if (readStmts) {
        ReadStmtGenerator readGenerator(_ast, _view, &_tree, &_variables);

        for (const Stmt* stmt : readStmts->stmts()) {
            readGenerator.generateStmt(stmt);
        }

        // Place joins on vars that have more than one input
        readGenerator.placeJoinsOnVars();

        // Place joins based on property expressions
        readGenerator.placePropertyExprJoins();

        // Place joins based on where predicates
        readGenerator.placePredicateJoins();

        // Place joins that generate the endpoint, and retrieve it
        currentNode = readGenerator.generateEndpoint();
    }

    // Generate update statements (optional)
    if (updateStmts) {
        WriteStmtGenerator writeGenerator(_ast, &_tree, &_variables);

        for (const Stmt* stmt : updateStmts->stmts()) {
            currentNode = writeGenerator.generateStmt(stmt, currentNode);
        }
    }

    // Generate return statement
    if (returnStmt) {
        generateReturnStmt(returnStmt, currentNode);
    }
}

void PlanGraphGenerator::generateReturnStmt(const ReturnStmt* stmt, PlanGraphNode* prevNode) {
    if (prevNode == nullptr) {
        throwError("Return statement without previous node", stmt);
    }

    _tree.newOut<ProduceResultsNode>(prevNode);
}

void PlanGraphGenerator::throwError(std::string_view msg, const void* obj) const {
    throw PlannerException(_ast->createErrorString(msg, obj));
}
