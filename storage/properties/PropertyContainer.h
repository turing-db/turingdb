#pragma once

#include <limits>
#include <optional>
#include <unordered_map>

#include <range/v3/algorithm/sort.hpp>
#include <range/v3/view/zip.hpp>

#include "EmbeddingContainer.h"
#include "StringContainer.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "ID.h"

namespace db {

template <SupportedType T>
class TypedPropertyContainer;

template <SupportedType T>
class TrivialPropertyContainerLoader;

class StringPropertyContainerLoader;
class EmbeddingPropertyContainerLoader;

class PropertyContainer {
public:
    using IDs = std::vector<EntityID>;

    explicit PropertyContainer(ValueType valueType)
        : _valueType(valueType)
    {
    }

    PropertyContainer(const PropertyContainer&) = delete;
    PropertyContainer(PropertyContainer&&) = default;
    PropertyContainer& operator=(const PropertyContainer&) = delete;
    PropertyContainer& operator=(PropertyContainer&&) = default;
    virtual ~PropertyContainer() = default;

    template <SupportedType T>
    TypedPropertyContainer<T>& cast() {
        return *static_cast<TypedPropertyContainer<T>*>(this);
    }

    template <SupportedType T>
    const TypedPropertyContainer<T>& cast() const {
        return *static_cast<const TypedPropertyContainer<T>*>(this);
    }

    virtual void sort() = 0;
    virtual size_t size() const = 0;

    virtual bool has(EntityID entityID) const = 0;
    ValueType getValueType() const { return _valueType; }

    IDs& ids() { return _ids; }
    const IDs& ids() const { return _ids; }

    IDs& nullIds() { return _nullIds; }
    const IDs& nullIds() const { return _nullIds; }

protected:
    IDs _ids;
    IDs _nullIds;
    std::unordered_map<EntityID, size_t> _entityIndexMap;
    static constexpr size_t NULL_INDEX = std::numeric_limits<size_t>::max();

private:
    ValueType _valueType {ValueType::Invalid};
};

template <SupportedType T>
class TypedPropertyContainer : public PropertyContainer {
public:
    using Values = std::vector<typename T::Primitive>;

    TypedPropertyContainer()
        : PropertyContainer(T::_valueType)
    {
    }

    TypedPropertyContainer(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer(TypedPropertyContainer&&) noexcept = default;
    TypedPropertyContainer& operator=(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer& operator=(TypedPropertyContainer&&) noexcept = default;
    ~TypedPropertyContainer() override = default;

    void add(EntityID entityID, const std::optional<typename T::Primitive>& arg) {
        if (!arg.has_value()) {
            _nullIds.emplace_back(entityID);
            _entityIndexMap[entityID] = NULL_INDEX;
            return;
        }

        const size_t index = _values.size();
        _values.push_back(*arg);
        _ids.emplace_back(entityID);
        _entityIndexMap[entityID] = index;
    }

    bool has(EntityID entityID) const override {
        auto it = find(entityID);
        return it != _values.end();
    }

    Values::const_iterator find(EntityID id) const {
        const auto it = _entityIndexMap.find(id);

        if (it == _entityIndexMap.end()) {
            return _values.end();
        }

        const size_t offset = it->second;

        if (offset == NULL_INDEX) {
            return _values.end();
        }

        return _values.begin() + offset;
    }

    const T::Primitive& get(EntityID entityID) const {
        const auto it = find(entityID);
        return *it;
    }

    const T::Primitive& get(size_t offset) const {
        return _values[offset];
    }

    std::span<const typename T::Primitive> all() const {
        return _values;
    }

    const T::Primitive* tryGet(EntityID entityID) const {
        auto it = find(entityID);
        if (it == _values.end()) {
            return nullptr;
        }
        return &(*it);
    }

    /**
     * @brief Gets the (possibly null) value of the property associated with @param
     * entityID.
     * @returns
     * - std::nullopt: if the value is explicitly null;
     * - nullptr:      if there is no associated value;
     * - the value:    otherwise.
     */
    std::optional<const typename T::Primitive*> tryGetWithNull(EntityID entityID) const {
        const auto findIt = _entityIndexMap.find(entityID);

        const bool present = findIt != _entityIndexMap.end();
        if (!present) {
            return nullptr;
        }

        const size_t offset = findIt->second;

        const bool explicitNull = offset == NULL_INDEX;
        if (explicitNull) {
            return std::nullopt;
        }

        const auto valueIt = _values.begin() + offset;

        return &(*valueIt);
    }

    std::span<const typename T::Primitive> getSpan(size_t first, size_t count) const {
        return std::span {_values}.subspan(first, count);
    }

    Values::const_iterator begin() const { return _values.begin(); }
    Values::const_iterator end() const { return _values.end(); }

    auto zipped() const { return ranges::views::zip(_ids, _values); }

    size_t size() const override {
        return _values.size();
    }

    void sort() override {
        ranges::sort(
            ranges::views::zip(_ids, _values),
            [&](const auto& pair1, const auto& pair2) {
                const EntityID id1 = std::get<0>(pair1);
                const EntityID id2 = std::get<0>(pair2);
                return id1 < id2;
            });

        _entityIndexMap.clear();
        _entityIndexMap.reserve(_ids.size() + _nullIds.size());
        for (size_t i = 0; i < _ids.size(); i++) {
            _entityIndexMap[_ids[i]] = i;
        }
        for (const EntityID id : _nullIds) {
            _entityIndexMap[id] = NULL_INDEX;
        }
    }

    Values& values() { return _values; }
    const Values& values() const { return _values; }

private:
    friend TrivialPropertyContainerLoader<T>;

    Values _values;
};

template <>
class TypedPropertyContainer<types::String> : public PropertyContainer {
public:
    using Values = std::vector<typename types::String::Primitive>;

    TypedPropertyContainer()
        : PropertyContainer(types::String::_valueType)
    {
    }

    TypedPropertyContainer(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer(TypedPropertyContainer&&) noexcept = default;
    TypedPropertyContainer& operator=(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer& operator=(TypedPropertyContainer&&) noexcept = default;
    ~TypedPropertyContainer() override = default;

    void add(EntityID entityID, const std::optional<types::String::Primitive>& arg) {
        if (!arg.has_value()) {
            _nullIds.emplace_back(entityID);
            _entityIndexMap[entityID] = NULL_INDEX;
            return;
        }

        const size_t index = _values.size();
        _values.alloc(*arg);
        _ids.emplace_back(entityID);
        _entityIndexMap[entityID] = index;
    }

    bool has(EntityID entityID) const override {
        const auto it = find(entityID);
        return it != _values.end();
    }

    Values::const_iterator find(EntityID id) const {
        auto it = _entityIndexMap.find(id);

        if (it == _entityIndexMap.end()) {
            return _values.end();
        }

        const size_t offset = it->second;

        return _values.begin() + offset;
    }

    const std::string_view& get(EntityID entityID) const {
        const auto it = find(entityID);
        return *it;
    }

    const std::string_view& get(size_t offset) const {
        return _values.getView(offset);
    }

    const StringContainer& getRawContainer() const {
        return _values;
    }

    std::span<const std::string_view> all() const {
        return _values.get();
    }

    const std::string_view* tryGet(EntityID entityID) const {
        auto it = find(entityID);
        if (it == _values.end()) {
            return nullptr;
        }
        return &(*it);
    }

    std::optional<const types::String::Primitive*> tryGetWithNull(EntityID entityID) const {
        const auto findIt = _entityIndexMap.find(entityID);

        const bool present = findIt != _entityIndexMap.end();
        if (!present) {
            return nullptr;
        }

        const size_t offset = findIt->second;

        const bool explicitNull = offset == NULL_INDEX;
        if (explicitNull) {
            return std::nullopt;
        }

        const auto valueIt = _values.begin() + offset;

        return &(*valueIt);
    }

    std::span<const std::string_view> getSpan(size_t first, size_t count) const {
        return std::span {_values.get()}.subspan(first, count);
    }

    Values::const_iterator begin() const { return _values.begin(); }
    Values::const_iterator end() const { return _values.end(); }

    auto zipped() const { return ranges::views::zip(_ids, _values.get()); }

    size_t size() const override {
        return _values.size();
    }

    void sort() override;

private:
    friend StringPropertyContainerLoader;
    friend DataPartMerger;

    StringContainer _values;
};

template <>
class TypedPropertyContainer<types::Embedding> : public PropertyContainer {
public:
    explicit TypedPropertyContainer(size_t dimension)
        : PropertyContainer(types::Embedding::_valueType),
        _values(dimension)
    {
    }

    TypedPropertyContainer(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer(TypedPropertyContainer&&) noexcept = default;
    TypedPropertyContainer& operator=(const TypedPropertyContainer&) = delete;
    TypedPropertyContainer& operator=(TypedPropertyContainer&&) noexcept = default;
    ~TypedPropertyContainer() override = default;

    void add(EntityID entityID, const std::optional<types::Embedding::Primitive>& arg) {
        if (!arg.has_value()) {
            _nullIds.emplace_back(entityID);
            _entityIndexMap[entityID] = NULL_INDEX;
            return;
        }

        const size_t index = _values.size();
        _values.alloc(*arg);
        _ids.emplace_back(entityID);
        _entityIndexMap[entityID] = index;
    }

    bool has(EntityID entityID) const override {
        return _entityIndexMap.contains(entityID);
    }

    types::Embedding::Primitive get(EntityID entityID) const {
        const auto it = _entityIndexMap.find(entityID);
        return _values.getView(it->second);
    }

    types::Embedding::Primitive get(size_t offset) const {
        return _values.getView(offset);
    }

    const types::Embedding::Primitive* tryGet(EntityID entityID) const {
        const auto it = _entityIndexMap.find(entityID);
        if (it == _entityIndexMap.end()) {
            return nullptr;
        }
        const auto& views = _values.get();
        return &views[it->second];
    }

    std::optional<const types::Embedding::Primitive*> tryGetWithNull(EntityID entityID) const {
        const auto findIt = _entityIndexMap.find(entityID);

        const bool present = findIt != _entityIndexMap.end();
        if (!present) {
            return nullptr;
        }

        const size_t offset = findIt->second;

        const bool explicitNull = offset == NULL_INDEX;
        if (explicitNull) {
            return std::nullopt;
        }

        const auto& views = _values.get();

        return &views[offset];
    }

    std::span<const types::Embedding::Primitive> all() const {
        const auto& views = _values.get();
        return views;
    }

    std::span<const types::Embedding::Primitive> getSpan(size_t first, size_t count) const {
        const auto& views = _values.get();
        return std::span{views}.subspan(first, count);
    }

    const EmbeddingContainer& getRawContainer() const {
        return _values;
    }

    auto zipped() const { return ranges::views::zip(_ids, _values.get()); }

    size_t size() const override { return _values.size(); }

    void sort() override;

private:
    friend EmbeddingPropertyContainerLoader;
    friend DataPartMerger;

    EmbeddingContainer _values;
};

}
