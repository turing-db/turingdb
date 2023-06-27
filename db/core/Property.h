#pragma once

#include "Comparator.h"
#include "Value.h"

namespace db {

class PropertyType;

class Property {
public:
    friend DBComparator;

    Property(const PropertyType* type, Value value);
    Property();
    ~Property();

    bool isValid() const { return _type && _value.isValid(); }

    const PropertyType* getType() const { return _type; }
    Value getValue() const { return _value; }

    void setType(const PropertyType* type) { _type = type; }
    void setValue(const Value& value) { _value = value; }

private:
    const PropertyType* _type {nullptr};
    Value _value;
};

}
