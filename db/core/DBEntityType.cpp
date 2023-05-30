#include "DBEntityType.h"

#include "PropertyType.h"

using namespace db;

DBEntityType::DBEntityType(DBIndex index, StringRef name)
    : DBType(index, name)
{
}

DBEntityType::~DBEntityType() {
    for (const auto& propTypeEntry : _propTypes) {
        delete propTypeEntry.second;
    }
    _propTypes.clear();
}

DBEntityType::PropertyTypeRange DBEntityType::propertyTypes() const {
    return PropertyTypeRange(&_propTypes);
}

PropertyType* DBEntityType::getPropertyType(StringRef name) const {
    const auto it = _propTypes.find(name); 
    if (it == _propTypes.end()) {
        return nullptr;
    }

    return it->second;
}

void DBEntityType::addPropertyType(PropertyType* propType) {
    _propTypes[propType->getName()] = propType;
}
