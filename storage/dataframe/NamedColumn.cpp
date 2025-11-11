#include "NamedColumn.h"

#include "DataframeManager.h"

using namespace db;

NamedColumn* NamedColumn::create(DataframeManager* dfMan,
                                 Column* column,
                                 ColumnTag tag) {
    NamedColumn* namedCol = new NamedColumn(dfMan, tag, column);
    dfMan->addColumn(namedCol);
    return namedCol;
}

std::string_view NamedColumn::getName() const {
    return _dfMan->getTagManager().getName(_tag);
}

void NamedColumn::rename(std::string_view name) {
    _dfMan->getTagManager().setTagName(_tag, name);
}
