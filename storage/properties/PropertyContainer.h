#pragma once

#include <numeric>
#include <range/v3/algorithm/sort.hpp>
#include <range/v3/view/zip.hpp>

#include "StringContainer.h"
#include "metadata/PropertyType.h"

namespace db {

template <SupportedType T>
class TypedPropertyContainer;

template <SupportedType T>
class TrivialPropertyContainerLoader;

class StringPropertyContainerLoader;

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

protected:
    IDs _ids;

private:
    ValueType _valueType;
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

    template <typename... Args>
    void add(EntityID entityID, Args&&... args) {
        _values.emplace_back(std::forward<Args>(args)...);
        _ids.emplace_back(entityID);
    }

    bool has(EntityID entityID) const override {
        auto it = find(entityID);
        return it != _values.end();
    }

    Values::const_iterator find(EntityID id) const {
        auto it = std::lower_bound(_ids.begin(),
                                   _ids.end(),
                                   id);
        if (it == _ids.end()) {
            return _values.end();
        }

        const size_t index = std::distance(_ids.begin(), it);

        if (_ids[index] != id) {
            return _values.end();
        }

        return _values.begin() + index;
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

    void add(EntityID entityID, std::string_view v) {
        _values.alloc(v);
        _ids.emplace_back(entityID);
    }

    bool has(EntityID entityID) const override {
        const auto it = find(entityID);
        return it != _values.end();
    }

    Values::const_iterator find(EntityID id) const {
        auto it = std::lower_bound(_ids.begin(),
                                   _ids.end(),
                                   id);
        if (it == _ids.end()) {
            return _values.end();
        }

        const size_t index = std::distance(_ids.begin(), it);

        if (_ids[index] != id) {
            return _values.end();
        }

        return _values.begin() + index;
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

    std::span<const std::string_view> getSpan(size_t first, size_t count) const {
        return std::span {_values.get()}.subspan(first, count);
    }

    Values::const_iterator begin() const { return _values.begin(); }
    Values::const_iterator end() const { return _values.end(); }

    auto zipped() const { return ranges::views::zip(_ids, _values.get()); }

    size_t size() const override {
        return _values.size();
    }

    void sort() override {
        StringContainer newValues;
        if (_ids.empty()) {
            return;
        }

        std::vector<size_t> offsets(_ids.size());
        std::iota(offsets.begin(), offsets.end(), 0);

        ranges::sort(
            ranges::views::zip(_ids, offsets),
            [&](const auto& pair1, const auto& pair2) {
                const EntityID id1 = std::get<0>(pair1);
                const EntityID id2 = std::get<0>(pair2);
                return id1 < id2;
            });

        for (size_t i : offsets) {
            newValues.alloc(_values.getView(i));
        }

        _values = std::move(newValues);
    }

private:
    friend StringPropertyContainerLoader;
    friend DataPartMerger;

    StringContainer _values;
};
}
