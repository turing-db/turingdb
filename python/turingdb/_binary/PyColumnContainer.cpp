#include "PyColumnContainer.h"

using namespace pybindings;

PyColumnContainer::PyColumnContainer() {
}

PyColumnContainer::~PyColumnContainer() {
}

void PyColumnContainer::addColumn(PyColumn* column, std::string_view name) {
    _columns.push_back(column);
    _names.emplace_back(name);
}
