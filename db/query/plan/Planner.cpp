#include "Planner.h"

#include "QueryCommand.h"
#include "LogicalOperator.h"
#include "Value.h"
#include "SymbolTable.h"
#include "PullPlan.h"

using namespace db::query;
using namespace db;

Planner::Planner()
{
}

Planner::~Planner() {
}

PullPlan* Planner::makePlan(const QueryCommand* cmd) {
    switch (cmd->getKind()) {
        case QueryCommand::QCOM_LIST_COMMAND:
            return planListCommand(static_cast<const ListCommand*>(cmd));

        default:
            return nullptr;
    }
    return nullptr;
}

PullPlan* Planner::planListCommand(const ListCommand* cmd) {
    switch (cmd->getSubType()) {
        case ListCommand::LCOM_DATABASES:
            return planListDatabases(cmd);

        default:
            return nullptr;
    }

    return nullptr;
}

PullPlan* Planner::planListDatabases(const ListCommand* cmd) {
    const auto callback = [](Frame& frame,
                             ExecutionContext* ctxt,
                             std::vector<std::vector<Value>>& rows) {
        rows.clear();
    };

    SymbolTable* symTable = new SymbolTable();
    std::vector<Symbol*> outputSymbols;
    outputSymbols.push_back(symTable->createSymbol("name"));

    OutputTableOperator* plan = new OutputTableOperator(outputSymbols, callback);
    return new PullPlan(plan, symTable);
}
