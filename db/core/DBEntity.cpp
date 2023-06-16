#include "DBEntity.h"

#include "PropertyType.h"

using namespace db;

DBEntity::DBEntity(DBIndex index, DBEntityType* type)
    : DBObject(index), _type(type)
{
}

DBEntity::~DBEntity() {
}

Property DBEntity::getProperty(const PropertyType* propType) const {
    const auto it = _properties.find(propType);
    if (it == _properties.end()) {
        return Property();
    }

    return Property(it->first, it->second);
}

DBEntity::PropertyRange DBEntity::properties() const {
    return PropertyRange {&_properties};
}

void DBEntity::addProperty(const Property& prop) {
    _properties[prop.getType()] = prop.getValue();
}
