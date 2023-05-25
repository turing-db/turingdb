#pragma once

#include "DBType.h"

#include "ValueType.h"

namespace db {

class Writeback;

class PropertyType : public DBType {
public:
    friend Writeback;

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
