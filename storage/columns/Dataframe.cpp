#include "Dataframe.h"

#include <spdlog/fmt/fmt.h>

#include "NamedColumn.h"

#include "TuringException.h"

using namespace db;

namespace {

void throwColumnAlreadyExists(NamedColumn* col, ColumnName name) {
    throw TuringException(fmt::format("Dataframe already has a column with name {}", name.toStdString()));
}

}

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
        throwColumnAlreadyExists(column, primaryName);
    }

    _cols.push_back(column);
    _headerMap.addNameForColumn(column, primaryName);
}

void Dataframe::addNameToColumn(NamedColumn* col, ColumnName name) {
    if (col->getPrimaryName() != name && getColumn(name)) {
        throwColumnAlreadyExists(col, name);
    }

    _headerMap.addNameForColumn(col, name);
}

void Dataframe::setColumnPrimaryName(NamedColumn* col, ColumnName newName) {
    const ColumnName currentPrimaryName = col->getPrimaryName();
    if (currentPrimaryName != newName && getColumn(newName)) {
        throwColumnAlreadyExists(col, newName);
    }

    _headerMap.removeColumnName(currentPrimaryName);
    _headerMap.addNameForColumn(col, newName);
    col->_primaryName = newName;
}
