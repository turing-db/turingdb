#pragma once

#include <map>

#include "DBObject.h"

#include "Property.h"

namespace db {

class PropertyType;
class DBEntityType;
class Writeback;

class DBEntity : public DBObject {
public:
    friend Writeback;

    Property getProperty(const PropertyType* propType) const;

    DBEntityType* getType() const { return _type; }

protected:
    DBEntity(DBIndex index, DBEntityType* type);
    virtual ~DBEntity();

private:
    DBEntityType* _type {nullptr};
    std::map<const PropertyType*, Value, DBObject::Comparator> _properties;

    void addProperty(const Property& prop);
};

}
