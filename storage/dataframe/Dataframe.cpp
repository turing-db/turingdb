#include "Dataframe.h"

#include "NamedColumn.h"

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
