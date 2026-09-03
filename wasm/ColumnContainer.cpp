#include "ColumnContainer.h"

#include "Column.h"

using namespace wasm;

ColumnContainer::ColumnContainer()
{
}

ColumnContainer::~ColumnContainer() {
    clear();
}

void ColumnContainer::addColumn(Column* column, std::string_view name) {
    _columns.push_back(column);
    _names.emplace_back(name);
}

Column* ColumnContainer::operator[](size_t index) {
    return _columns.at(index);
}

const std::string& ColumnContainer::getName(size_t index) const {
    return _names.at(index);
}

void ColumnContainer::clear() {
    for (Column* column : _columns) {
        delete column;
    }

    _columns.clear();
    _names.clear();
}

void ColumnContainer::clearColumnData() {
    for (Column* column : _columns) {
        column->clear();
    }
}
