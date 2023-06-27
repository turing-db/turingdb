#pragma once

#include <map>

#include "Comparator.h"
#include "DBType.h"

#include "PropertyType.h"
#include "StringRef.h"
#include "Range.h"

namespace db {

class Writeback;

class DBEntityType : public DBType {
public:
    friend Writeback;
    friend DBComparator;
    using PropertyTypes = std::map<StringRef, PropertyType*>;
    using PropertyTypeRange = STLIndexRange<PropertyTypes>;

    PropertyType* getPropertyType(StringRef name) const;

    PropertyTypeRange propertyTypes() const;

protected:
    DBEntityType(DBIndex index, StringRef name);
    virtual ~DBEntityType();
    void addPropertyType(PropertyType* propType);

private:
    PropertyTypes _propTypes;
};

}
