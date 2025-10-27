#include "NamedColumn.h"

#include <spdlog/fmt/fmt.h>

#include "Dataframe.h"

#include "TuringException.h"

using namespace db;

NamedColumn* NamedColumn::create(Dataframe* parent,
                                 Column* column,
                                 ColumnName name) {
    if (parent->getColumn(name)) {
        throw TuringException(fmt::format("A column with name {} already exists in dataframe", name.toStdString()));
    }

    NamedColumn* namedCol = new NamedColumn(parent, name, column);
    parent->addColumn(namedCol);
    return namedCol;
}

void NamedColumn::setPrimaryName(ColumnName name) {
    _parent->setColumnPrimaryName(this, name);
}
