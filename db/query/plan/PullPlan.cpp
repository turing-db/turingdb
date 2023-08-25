#include "PullPlan.h"

#include "LogicalOperator.h"
#include "SymbolTable.h"

#include "DBService.grpc.pb.h"

using namespace db::query;

PullPlan::PullPlan(LogicalOperator* plan, SymbolTable* symTable)
    : _plan(plan),
    _symTable(symTable),
    _frame(symTable->size()),
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

bool PullPlan::pull(PullResponse* res) {
    _cursor->pull(_frame, nullptr);

    const auto& outputSymbols = _plan->getOutputSymbols();

    Record* record = res->add_records();
    for (Symbol* symbol : outputSymbols) {
        const auto& value = _frame[symbol];
        if (!value.isValid()) {
            continue;
        }

        Cell* cell = record->add_cells();
        encodeValue(cell, value);
    }

    return true;
}

void PullPlan::encodeValue(Cell* cell, const db::Value& value) {
}
