#include "Dataframe.h"

#include "NamedColumn.h"
#include "columns/Column.h"

using namespace db;

Dataframe::Dataframe()
{
}

Dataframe::~Dataframe() {
    for (NamedColumn* col : _cols) {
        delete col;
    }
}

void Dataframe::addColumn(NamedColumn* column) {
    _cols.push_back(column);
    _headerMap[column->getHeader().getTag()] = column;
}

size_t Dataframe::getRowCount() const {
    if (_cols.empty()) {
        return 0;
    }

    return _cols[0]->getColumn()->size();
}
