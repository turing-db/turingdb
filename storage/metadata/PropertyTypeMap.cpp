#include "PropertyTypeMap.h"

#include "BioAssert.h"

using namespace db;

PropertyTypeMap::PropertyTypeMap() = default;

PropertyTypeMap::~PropertyTypeMap() = default;

PropertyTypeMap::PropertyTypeMap(const PropertyTypeMap& other)
{
    for (const auto& pair : other._container) {
        const PropertyType& pt = pair._pt;
        const auto& name = pair._name;
        const size_t count = _container.size();
        auto newName = std::make_unique<std::string>(*name);
        auto* namePtr = newName.get();
        _container.emplace_back(pt, std::move(newName), pair._embeddingDimension);
        _nameMap.emplace(*namePtr, count);
        _idMap.emplace(pt._id, count);
    }
}

PropertyTypeMap& PropertyTypeMap::operator=(const PropertyTypeMap& other) {
    if (this == &other) {
        return *this;
    }

    _nameMap.clear();
    _idMap.clear();
    _container.clear();

    for (const auto& pair : other._container) {
        const PropertyType& pt = pair._pt;
        const auto& name = pair._name;
        const size_t count = _container.size();
        auto newName = std::make_unique<std::string>(*name);
        auto* namePtr = newName.get();
        _container.emplace_back(pt, std::move(newName), pair._embeddingDimension);
        _nameMap.emplace(*namePtr, count);
        _idMap.emplace(pt._id, count);
    }

    return *this;
}

std::optional<PropertyType> PropertyTypeMap::get(PropertyTypeID ptID) const {
    auto it = _idMap.find(ptID);
    if (it == _idMap.end()) {
        return std::nullopt;
    }

    const size_t offset = it->second;

    return _container[offset]._pt;
}

std::optional<PropertyType> PropertyTypeMap::get(const std::string_view name) const {
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

std::optional<size_t> PropertyTypeMap::getEmbeddingDimension(PropertyTypeID ptID) const {
    const auto it = _idMap.find(ptID);
    if (it == _idMap.end()) {
        return std::nullopt;
    }

    const size_t offset = it->second;

    return _container[offset]._embeddingDimension;
}

size_t PropertyTypeMap::getCount() const {
    return _nameMap.size();
}

PropertyType PropertyTypeMap::getOrCreate(std::string_view name, ValueType valueType) {
    auto it = _nameMap.find(name);
    if (it != _nameMap.end()) {
        return _container[it->second]._pt;
    }

    const size_t count = _nameMap.size();
    const PropertyTypeID nextID(static_cast<PropertyTypeID::Type>(count));
    const PropertyType pt(nextID, valueType);
    auto& pair = _container.emplace_back(pt, std::make_unique<std::string>(name));
    _nameMap.emplace(std::string_view {*pair._name}, count);
    _idMap.emplace(nextID, count);

    return pt;
}

size_t PropertyTypeMap::getOrSetEmbeddingDimension(PropertyTypeID ptID, size_t dimension) {
    const auto it = _idMap.find(ptID);
    bioassert(it != _idMap.end(), "Cannot fix the dimension of a property the map does not hold");

    Pair& pair = _container[it->second];
    bioassert(pair._pt._valueType == ValueType::Embedding, "Cannot fix the dimension of a property that holds no embeddings");

    if (!pair._embeddingDimension.has_value()) {
        pair._embeddingDimension = dimension;
    }

    return *pair._embeddingDimension;
}
