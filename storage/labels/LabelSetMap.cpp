#include "LabelSetMap.h"

using namespace db;

LabelSetMap::LabelSetMap() = default;

LabelSetMap::LabelSetMap(const LabelSetMap& other) {
    for (const auto& pair : other._container) {
        const LabelSetID id = pair._id;
        const auto& labelset = pair._value;
        const size_t count = _container.size();
        _container.emplace_back(id, std::make_unique<LabelSet>(*labelset));
        _valueMap.emplace(LabelSetHandle {id, *pair._value}, count);
        _idMap.emplace(id, count);
    }
}

LabelSetMap& LabelSetMap::operator=(const LabelSetMap& other) {
    if (this == &other) {
        return *this;
    }

    _valueMap.clear();
    _idMap.clear();
    _container.clear();

    for (const auto& pair : other._container) {
        const LabelSetID id = pair._id;
        const auto& labelset = pair._value;
        const size_t count = _container.size();
        _container.emplace_back(id, std::make_unique<LabelSet>(*labelset));
        _valueMap.emplace(LabelSetHandle {id, *pair._value}, count);
        _idMap.emplace(id, count);
    }

    return *this;
}

LabelSetMap::~LabelSetMap() = default;

std::optional<LabelSetID> LabelSetMap::get(const LabelSet& labelset) const {
    auto it = _valueMap.find(labelset);
    if (it == _valueMap.end()) {
        return std::nullopt;
    }

    return _container[it->second]._id;
}

std::optional<LabelSetID> LabelSetMap::get(const LabelSetHandle& labelset) const {
    auto it = _valueMap.find(labelset);
    if (it == _valueMap.end()) {
        return std::nullopt;
    }

    return _container[it->second]._id;
}

std::optional<LabelSetHandle> LabelSetMap::getValue(LabelSetID id) const {
    auto it = _idMap.find(id);
    if (it == _idMap.end()) {
        return std::nullopt;
    }

    return LabelSetHandle {id, *_container[it->second]._value};
}

size_t LabelSetMap::getCount() const {
    return _valueMap.size();
}

LabelSetHandle LabelSetMap::getOrCreate(const LabelSet& labelset) {
    auto it = _valueMap.find(labelset);
    if (it != _valueMap.end()) {
        const LabelSetID id = _idMap.at(it->second);
        return LabelSetHandle {id, *_container[it->second]._value};
    }

    const size_t count = _valueMap.size();
    const LabelSetID nextID {static_cast<LabelSetID::Type>(count)};
    auto& pair = _container.emplace_back(nextID, std::make_unique<LabelSet>(labelset));
    const LabelSetHandle& ref = _valueMap.emplace(LabelSetHandle {nextID, *pair._value}, count).first->first;
    _idMap.emplace(nextID, count);

    return ref;
}
