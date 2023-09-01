#pragma once

#include "Frame.h"
#include "ExecutionContext.h"

class PullResponse;
class Cell;

namespace db {
class Value;
}

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

    bool pull(PullResponse* res);

private:
    LogicalOperator* _plan {nullptr};
    SymbolTable* _symTable {nullptr};
    ExecutionContext _executionCtxt;
    Frame _frame;
    Cursor* _cursor {nullptr};

    void encodeValue(Cell* cell, const db::Value& value);
};

}
