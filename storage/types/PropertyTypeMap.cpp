#include "PropertyTypeMap.h"
#include "BioAssert.h"

#include <mutex>
#include <shared_mutex>

using namespace db;

PropertyTypeMap::PropertyTypeMap() = default;

PropertyTypeMap::~PropertyTypeMap() = default;

PropertyType PropertyTypeMap::get(const std::string& name) const {
    std::shared_lock guard(_lock);
    auto it = _offsetMap.find(name);
    if (it == _offsetMap.end()) {
        return PropertyType {};
    }
    return _propTypes[it->second];
}

PropertyType PropertyTypeMap::get(PropertyTypeID id) const {
    std::shared_lock guard(_lock);
    if (_propTypes.size() <= id.getValue()) {
        return PropertyType {};
    }

    return _propTypes[id.getValue()];
}

std::string_view PropertyTypeMap::getName(PropertyTypeID id) const {
    std::shared_lock guard(_lock);
    auto it = _idMap.find(id);
    msgbioassert(it != _idMap.end(), "Property type does not exist");
    return it->second;
}

size_t PropertyTypeMap::getCount() const {
    std::shared_lock guard(_lock);
    return _propTypes.size();
}

PropertyType PropertyTypeMap::getOrCreate(const std::string& name, ValueType valueType) {
    std::unique_lock guard(_lock);
    if (!unsafeExists(name)) {
        return unsafeCreate(name, valueType);
    }

    return _propTypes[_offsetMap.at(name)];
}

bool PropertyTypeMap::tryCreate(std::string&& name, ValueType valueType) {
    std::unique_lock guard(_lock);
    if (!unsafeExists(name)) {
        unsafeCreate(std::move(name), valueType);
        return true;
    }

    return false;
}

PropertyType PropertyTypeMap::create(std::string name,
                                     ValueType valueType) {
    std::unique_lock guard(_lock);
    return unsafeCreate(std::move(name), valueType);
}

PropertyType PropertyTypeMap::unsafeCreate(std::string name,
                                           ValueType valueType) {
    const PropertyTypeID nextID = _offsetMap.size();
    const std::string& v = _offsetMap.emplace(std::move(name), _propTypes.size()).first->first;
    auto& propType = _propTypes.emplace_back();
    propType._valueType = valueType;
    propType._id = nextID;
    _idMap[nextID] = v;

    return propType;
}

bool PropertyTypeMap::unsafeExists(const std::string& name) const {
    return _offsetMap.find(name) != _offsetMap.end();
}
