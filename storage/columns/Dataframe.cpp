#include "Dataframe.h"

#include <spdlog/fmt/fmt.h>

#include "NamedColumn.h"

#include "TuringException.h"

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
    const ColumnName primaryName = column->getPrimaryName();
    if (getColumn(primaryName)) {
        throw TuringException(fmt::format("Dataframe already has a column with name {}", primaryName.toStdString()));
    }

    _cols.push_back(column);
    _headerMap.addNameForColumn(column, primaryName);
}
