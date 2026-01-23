#include "LabelMap.h"

#include "LabelSet.h"

#include "TuringException.h"

using namespace db;

LabelMap::LabelMap() = default;

LabelMap::~LabelMap() = default;

LabelMap::LabelMap(const LabelMap& other)
{
    for (const auto& pair : other._container) {
        const LabelID id = pair._id;
        const auto& name = pair._name;
        const size_t count = _container.size();
        auto newName = std::make_unique<std::string>(*name);
        auto* namePtr = newName.get();
        _container.emplace_back(id, std::move(newName));
        _nameMap.emplace(*namePtr, count);
        _idMap.emplace(id, count);
    }
}

LabelMap& LabelMap::operator=(const LabelMap& other) {
    if (this == &other) {
        return *this;
    }

    _nameMap.clear();
    _idMap.clear();
    _container.clear();

    for (const auto& pair : other._container) {
        const LabelID id = pair._id;
        const auto& name = pair._name;
        const size_t count = _container.size();
        auto newName = std::make_unique<std::string>(*name);
        auto* namePtr = newName.get();
        _container.emplace_back(id, std::move(newName));
        _nameMap.emplace(*namePtr, count);
        _idMap.emplace(id, count);
    }

    return *this;
}

std::optional<LabelID> LabelMap::get(std::string_view name) const {
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

LabelID LabelMap::getOrCreate(std::string_view name) {
    auto it = _nameMap.find(name);

    if (it != _nameMap.end()) {
        return _container[it->second]._id;
    }

    const size_t offset = _nameMap.size();
    if (offset >= LabelSet::IntegerSize * LabelSet::IntegerCount) {
        throw TuringException(fmt::format(
            "Attempted to create LabelID {}, which exceeds graph label capacity.",
            offset));
    }
    const LabelID nextID {static_cast<LabelID::Type>(offset)};
    auto& pair = _container.emplace_back(nextID, std::make_unique<std::string>(name));
    _nameMap.emplace(std::string_view {*pair._name}, offset);
    _idMap.emplace(nextID, offset);

    return nextID;
}
