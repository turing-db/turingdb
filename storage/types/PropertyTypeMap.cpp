#include "PropertyTypeMap.h"

using namespace db;

PropertyTypeMap::PropertyTypeMap() = default;

PropertyTypeMap::~PropertyTypeMap() = default;

std::optional<PropertyType> PropertyTypeMap::get(const std::string& name) const {
    auto it = _nameMap.find(name);
    if (it == _nameMap.end()) {
        return std::nullopt;
    }

    const size_t offset = it->second;

    return _container[offset]._pt;
}

std::optional<std::string_view> PropertyTypeMap::getName(PropertyTypeID ptID) const {
    auto it = _idMap.find(ptID);
    if (it == _idMap.end()) {
        return std::nullopt;
    }

    const size_t offset = it->second;

    return *_container[offset]._name;
}

size_t PropertyTypeMap::getCount() const {
    return _nameMap.size();
}

PropertyType PropertyTypeMap::getOrCreate(const std::string& name, ValueType valueType) {
    auto it = _nameMap.find(name);
    if (it != _nameMap.end()) {
        return _container[it->second]._pt;
    }

    const size_t count = _nameMap.size();
    const PropertyTypeID nextID {static_cast<PropertyTypeID::Type>(count)};
    const PropertyType pt {._id = nextID, ._valueType = valueType};
    auto& pair = _container.emplace_back(pt, std::make_unique<std::string>(name));
    _nameMap.emplace(std::string_view {*pair._name}, count);
    _idMap.emplace(nextID, count);

    return pt;
}
