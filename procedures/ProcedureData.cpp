#include "ProcedureData.h"

#include "columns/Column.h"
#include "columns/ColumnConst.h"

using namespace db;

ProcedureData::ProcedureData()
{
}

ProcedureData::~ProcedureData() {
}

size_t ProcedureData::getArgumentRowCount() const {
    for (const Column* column : _inputColumns) {
        if (!column) {
            continue;
        }

        const bool isRowAligned = column->getContainerKind() != ContainerKind::code<ColumnConst>();
        if (isRowAligned) {
            return column->size();
        }
    }

    return 1;
}

void ProcedureData::clearReturnColumns() {
    for (Column* column : _returnColumns) {
        if (column) {
            column->clear();
        }
    }
}
