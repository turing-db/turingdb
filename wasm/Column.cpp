#include "Column.h"

using namespace wasm;

Column::Column(ColumnKind kind, ColumnType type)
    : _kind(kind),
      _type(type)
{
}

Column::~Column() {
}
