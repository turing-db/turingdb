#include "NamedColumn.h"

#include <spdlog/fmt/fmt.h>

#include "Dataframe.h"

#include "TuringException.h"

using namespace db;

NamedColumn* NamedColumn::create(Dataframe* parent,
                                 Column* column,
                                 const ColumnHeader& header) {
    if (parent->getColumn(header.getTag())) {
        throw TuringException(fmt::format(
            "A column with tag {} already exists in dataframe",
            header.getTag().value()));
    }

    NamedColumn* namedCol = new NamedColumn(parent, header, column);
    parent->addColumn(namedCol);
    return namedCol;
}
