#include "ColumnConst.h"

using namespace db;

template <>
ColumnConst<PropertyNull>::ColumnConst()
    : Column(_staticKind),
    _empty(false)
{
}
