#include "PropertyType.h"

using namespace db;

PropertyType::PropertyType(DBIndex index,
                           DBObject* owner,
                           StringRef name,
                           ValueType valType)
    : DBType(index, name),
    _owner(owner),
    _valType(valType)
{
}

PropertyType::~PropertyType() {
}
