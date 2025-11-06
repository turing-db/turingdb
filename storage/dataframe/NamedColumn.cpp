#include "NamedColumn.h"

#include "DataframeManager.h"

using namespace db;

NamedColumn* NamedColumn::create(DataframeManager* dfMan,
                                 Column* column,
                                 const ColumnHeader& header) {
    NamedColumn* namedCol = new NamedColumn(header, column);
    dfMan->addColumn(namedCol);
    return namedCol;
}
