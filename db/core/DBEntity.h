#pragma once

#include <map>

#include "Comparator.h"
#include "DBObject.h"

#include "Property.h"
#include "Range.h"

namespace db {

class Writeback;
class DBEntityType;

class DBEntity : public DBObject {
public:
    friend Writeback;
    friend DBComparator;
    using Properties = std::map<const PropertyType*, Value, DBObject::Sorter>;
    using PropertyRange = STLRange<Properties>;

    Property getProperty(const PropertyType* propType) const;
    PropertyRange properties() const;

    DBEntityType* getType() const { return _type; }

protected:
    DBEntity(DBIndex index, DBEntityType* type);
    virtual ~DBEntity();

private:
    DBEntityType* _type {nullptr};
    Properties _properties;

    void addProperty(const Property& prop);
};

}
