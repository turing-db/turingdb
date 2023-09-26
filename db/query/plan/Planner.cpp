#include "Planner.h"

#include "QueryCommand.h"
#include "LogicalOperator.h"
#include "Value.h"
#include "SymbolTable.h"
#include "PullPlan.h"
#include "ExecutionContext.h"
#include "InterpreterContext.h"
#include "DBManager.h"

using namespace db;

Planner::Planner(InterpreterContext* interpCtxt)
    : _interpCtxt(interpCtxt)
{
}

Planner::~Planner() {
}

PullPlan* Planner::makePlan(const QueryCommand* cmd) {
    switch (cmd->getKind()) {
        case QueryCommand::QCOM_LIST_COMMAND:
            return planListCommand(static_cast<const ListCommand*>(cmd));

        case QueryCommand::QCOM_OPEN_COMMAND:
            return planOpenCommand(static_cast<const OpenCommand*>(cmd));

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
        auto interpCtxt = ctxt->getInterpreterContext();
        DBManager* dbMan = interpCtxt->getDBManager();

        std::vector<std::string> databases;
        dbMan->getDatabases(databases);
        for (auto& db : databases) {
            rows.push_back(std::vector{Value::createString(std::move(db))});
        }
    };

    SymbolTable* symTable = new SymbolTable();
    std::vector<Symbol*> outputSymbols;
    outputSymbols.push_back(symTable->createSymbol("name"));

    OutputTableOperator* plan = new OutputTableOperator(outputSymbols, callback);
    return new PullPlan(plan, symTable, _interpCtxt);
}

PullPlan* Planner::planOpenCommand(const OpenCommand* cmd) {
    SymbolTable* symTable = new SymbolTable();
    OpenDBOperator* openDB = new OpenDBOperator(cmd->getPath(), std::vector<Symbol*>());
    return new PullPlan(openDB, symTable, _interpCtxt);
}
