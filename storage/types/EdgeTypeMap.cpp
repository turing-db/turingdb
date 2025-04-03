#include "EdgeTypeMap.h"

using namespace db;

EdgeTypeMap::EdgeTypeMap() = default;

EdgeTypeMap::~EdgeTypeMap() = default;

std::optional<EdgeTypeID> EdgeTypeMap::get(const std::string& name) const {
    auto it = _nameMap.find(name);
    if (it == _nameMap.end()) {
        return std::nullopt;
    }

    return _container[it->second]._id;
}

std::optional<std::string_view> EdgeTypeMap::getName(EdgeTypeID id) const {
    auto it = _idMap.find(id);
    if (it == _idMap.end()) {
        return std::nullopt;
    }

    return *_container[it->second]._name;
}

size_t EdgeTypeMap::getCount() const {
    return _nameMap.size();
}

EdgeTypeID EdgeTypeMap::getOrCreate(const std::string& name) {
    auto it = _nameMap.find(name);
    if (it != _nameMap.end()) {
        return _container[it->second]._id;
    }

    const size_t count = _nameMap.size();
    const EdgeTypeID nextID {static_cast<EdgeTypeID::Type>(count)};
    auto& pair = _container.emplace_back(nextID, std::make_unique<std::string>(name));
    _nameMap.emplace(std::string_view {*pair._name}, count);
    _idMap.emplace(nextID, count);

    return nextID;
}
