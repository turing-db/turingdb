#pragma once

#include "Frame.h"
#include "ExecutionContext.h"
#include "PullStatus.h"

namespace db {

class LogicalOperator;
class SymbolTable;
class Cursor;

class PullPlan {
public:
    PullPlan(LogicalOperator* plan,
             SymbolTable* symTable,
             InterpreterContext* interpCtxt);
    ~PullPlan();

    PullStatus pull();

    SymbolTable* getSymbolTable() const { return _symTable; }
    const Frame& getFrame() const { return _frame; }

private:
    LogicalOperator* _plan {nullptr};
    SymbolTable* _symTable {nullptr};
    ExecutionContext _executionCtxt;
    Frame _frame;
    Cursor* _cursor {nullptr};
};

}
