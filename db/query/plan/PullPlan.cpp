#include "PullPlan.h"

#include "LogicalOperator.h"
#include "SymbolTable.h"

using namespace db;

PullPlan::PullPlan(LogicalOperator* plan,
                   SymbolTable* symTable,
                   InterpreterContext* interpCtxt)
    : _plan(plan),
    _symTable(symTable),
    _executionCtxt(interpCtxt),
    _frame(symTable),
    _cursor(_plan->makeCursor())
{
}

PullPlan::~PullPlan() {
    if (_plan) {
        delete _plan;
    }
    if (_symTable) {
        delete _symTable;
    }

    if (_cursor) {
        delete _cursor;
    }
}

PullStatus PullPlan::pull() {
    return _cursor->pull(_frame, &_executionCtxt);
}
