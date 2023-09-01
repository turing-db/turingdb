#include "PullPlan.h"

#include "LogicalOperator.h"
#include "SymbolTable.h"

#include "DBService.grpc.pb.h"

using namespace db::query;

PullPlan::PullPlan(LogicalOperator* plan,
                   SymbolTable* symTable,
                   InterpreterContext* interpCtxt)
    : _plan(plan),
    _symTable(symTable),
    _executionCtxt(interpCtxt),
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
    if (!_cursor->pull(_frame, &_executionCtxt)) {
        return true;
    }

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
    switch (value.getType().getKind()) {
        case ValueType::VK_INT:
            cell->set_int64(value.getInt());
            break;

        case ValueType::VK_UNSIGNED:
            cell->set_uint64(value.getUint());
            break;

        case ValueType::VK_BOOL:
            cell->set_boolean(value.getBool());
            break;

        case ValueType::VK_DECIMAL:
            cell->set_decimal(value.getDouble());
            break;

        case ValueType::VK_STRING_REF:
            cell->set_string(value.getStringRef().toStdString());
            break;

        case ValueType::VK_STRING:
            cell->set_string(value.getString());
            break;

        default:
            break;
    }
}
