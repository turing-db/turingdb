#pragma once

#include "DBType.h"

#include "ValueType.h"
#include "Comparator.h"

namespace db {

class Writeback;
class DBEntityType;

class PropertyType : public DBType {
public:
    friend DBEntityType;
    friend Writeback;
    friend DBComparator;

    ValueType getValueType() const { return _valType; }

private:
    DBObject* _owner {nullptr};
    ValueType _valType;

    PropertyType(DBIndex index,
                 DBObject* owner,
                 StringRef name,
                 ValueType valType);
    ~PropertyType();
};

}
