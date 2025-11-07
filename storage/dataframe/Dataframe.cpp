#include "Dataframe.h"

#include "NamedColumn.h"
#include "columns/Column.h"

using namespace db;

Dataframe::Dataframe()
{
}

Dataframe::~Dataframe() {
}

void Dataframe::addColumn(NamedColumn* column) {
    _cols.push_back(column);

    // Record column in the headerMap using its tag
    // Resize the table if needed
    const size_t tagValue = column->getTag().getValue();
    if (tagValue >= _headerMap.size()) {
        _headerMap.resize(tagValue+1);
    }

    _headerMap[tagValue] = column;
}

size_t Dataframe::getRowCount() const {
    if (_cols.empty()) {
        return 0;
    }

    return _cols[0]->getColumn()->size();
}

void Dataframe::dump(std::ostream& out) const {
    for (const NamedColumn* col : _cols) {
        out << "Column $" << col->getTag().getValue() << ": ";
        col->getColumn()->dump(out);
        out << '\n';
    }
}

void Dataframe::copyFromLine(const Dataframe* other, size_t startRow, size_t rowCount) {
    const NamedColumns& otherColumns = other->cols();
    const size_t copySize = std::min(_cols.size(), otherColumns.size());
    for (size_t i = 0; i < copySize; i++) {
        Column* otherColumn = otherColumns[i]->getColumn();
        Column* column = _cols[i]->getColumn();
        column->assignFromLine(otherColumn, startRow, rowCount);
    }
}

void Dataframe::copyFrom(const Dataframe* other) {
    const NamedColumns& otherColumns = other->cols();
    const size_t copySize = std::min(_cols.size(), otherColumns.size());
    for (size_t i = 0; i < copySize; i++) {
        Column* otherColumn = otherColumns[i]->getColumn();
        Column* column = _cols[i]->getColumn();
        column->assign(otherColumn);
    }
}
