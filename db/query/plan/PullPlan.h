#pragma once

#include "Frame.h"
#include "ExecutionContext.h"

namespace db::query {

class LogicalOperator;
class SymbolTable;
class Cursor;

class PullPlan {
public:
    PullPlan(LogicalOperator* plan,
             SymbolTable* symTable,
             InterpreterContext* interpCtxt);
    ~PullPlan();

    bool pull();

private:
    LogicalOperator* _plan {nullptr};
    SymbolTable* _symTable {nullptr};
    ExecutionContext _executionCtxt;
    Frame _frame;
    Cursor* _cursor {nullptr};
};

}
