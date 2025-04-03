#include "LabelMap.h"

using namespace db;

LabelMap::LabelMap() = default;

LabelMap::~LabelMap() = default;

std::optional<LabelID> LabelMap::get(const std::string& name) const {
    auto it = _nameMap.find(name);
    if (it == _nameMap.end()) {
        return std::nullopt;
    }

    return _container[it->second]._id;
}

std::optional<std::string_view> LabelMap::getName(LabelID id) const {
    auto it = _idMap.find(id);
    if (it == _idMap.end()) {
        return std::nullopt;
    }

    return *_container[it->second]._name;
}

size_t LabelMap::getCount() const {
    return _nameMap.size();
}

LabelID LabelMap::getOrCreate(const std::string& name) {
    auto it = _nameMap.find(name);

    if (it != _nameMap.end()) {
        return _container[it->second]._id;
    }

    const size_t count = _nameMap.size();
    const LabelID nextID {static_cast<LabelID::Type>(count)};
    auto& pair = _container.emplace_back(nextID, std::make_unique<std::string>(name));
    _nameMap.emplace(std::string_view {*pair._name}, count);
    _idMap.emplace(nextID, count);

    return nextID;
}
