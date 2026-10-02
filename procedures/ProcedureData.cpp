#include "ProcedureData.h"

#include "columns/Column.h"

using namespace db;

ProcedureData::ProcedureData()
{
}

ProcedureData::~ProcedureData() {
}

void ProcedureData::clearReturnColumns() {
    for (Column* column : _returnColumns) {
        if (column) {
            column->clear();
        }
    }
}
