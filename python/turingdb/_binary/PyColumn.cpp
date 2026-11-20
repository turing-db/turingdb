#include "PyColumn.h"

using namespace pybindings;

PyColumn::PyColumn(net::proto::ColumnInternalKind internalType, net::proto::ColumnKind kind)
    : _internalType(internalType),
    _kind(kind)
{
}

PyColumn::~PyColumn() {
}
