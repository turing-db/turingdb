#pragma once

#include <memory>
#include <unordered_map>

#include "metadata/PropertyType.h"
#include "ID.h"
#include "indexers/PropertyIndexer.h"
#include "PropertyContainer.h"

namespace db {

class EntityPropertyView;
class NodeContainer;
class DataPartLoader;
class DataPartRebaser;

class PropertyManager {
public:
    using PropertyContainerMap = std::unordered_map<PropertyTypeID,
                                                    std::unique_ptr<PropertyContainer>>;
    using PropertyContainerReferences = std::unordered_map<PropertyTypeID,
                                                           PropertyContainer*>;

    explicit PropertyManager();
    ~PropertyManager();

    PropertyManager(const PropertyManager&) = delete;
    PropertyManager(PropertyManager&&) = delete;
    PropertyManager& operator=(const PropertyManager&) = delete;
    PropertyManager& operator=(PropertyManager&&) = delete;

    template <SupportedType T>
    void registerPropertyType(PropertyTypeID ptID) {
        msgbioassert(_map.find(ptID) == _map.end(),
                     "Trying to register a type that was already registered");
        auto* ptr = new TypedPropertyContainer<T>;
        _map.emplace(ptID, static_cast<PropertyContainer*>(ptr));

        if constexpr (std::is_same_v<T, types::UInt64>) {
            _uint64s.emplace(ptID, static_cast<PropertyContainer*>(ptr));
        } else if constexpr (std::is_same_v<T, types::Int64>) {
            _int64s.emplace(ptID, static_cast<PropertyContainer*>(ptr));
        } else if constexpr (std::is_same_v<T, types::Double>) {
            _doubles.emplace(ptID, static_cast<PropertyContainer*>(ptr));
        } else if constexpr (std::is_same_v<T, types::String>) {
            _strings.emplace(ptID, static_cast<PropertyContainer*>(ptr));
        } else if constexpr (std::is_same_v<T, types::Bool>) {
            _bools.emplace(ptID, static_cast<PropertyContainer*>(ptr));
        }
    }

    template <SupportedType T, typename... Args>
    void add(PropertyTypeID ptID, EntityID entityID, Args&&... args) {
        TypedPropertyContainer<T>& container = getMutableContainer<T>(ptID);
        container.add(entityID, std::forward<Args>(args)...);
    }

    bool hasPropertyType(PropertyTypeID ptID) const {
        return _map.find(ptID) != _map.end();
    }

    bool has(PropertyTypeID ptID, EntityID entityID) const {
        auto containerIt = _map.find(ptID);
        if (containerIt == _map.end()) {
            return false;
        }
        return containerIt->second->has(entityID);
    }

    template <SupportedType T>
    bool has(PropertyTypeID ptID, EntityID entityID) const {
        const TypedPropertyContainer<T>& container = getContainer<T>(ptID);
        return container.has(entityID);
    }

    template <SupportedType T>
    const T::Pritimive& get(PropertyTypeID ptID, EntityID entityID) const {
        const TypedPropertyContainer<T>& container = getContainer<T>(ptID);
        return container.get(entityID);
    }

    template <SupportedType T>
    std::span<const typename T::Primitive> all(PropertyTypeID ptID) const {
        const TypedPropertyContainer<T>& container = getContainer<T>(ptID);
        return container.all();
    }

    template <SupportedType T>
    const T::Primitive* tryGet(PropertyTypeID ptID, EntityID entityID) const {
        const TypedPropertyContainer<T>& container = getContainer<T>(ptID);
        return container.tryGet(entityID);
    }

    template <SupportedType T>
    TypedPropertyContainer<T>& getMutableContainer(PropertyTypeID ptID) {
        msgbioassert(_map.find(ptID) != _map.end(),
                     "Trying to access a property type that was not registered");
        return _map.at(ptID)->cast<T>();
    }

    template <SupportedType T>
    const TypedPropertyContainer<T>& getContainer(PropertyTypeID ptID) const {
        msgbioassert(_map.find(ptID) != _map.end(),
                     "Trying to access a property type that was not registered");
        return _map.at(ptID)->cast<T>();
    }

    void fillEntityPropertyView(EntityID entityID,
                                const LabelSetHandle& labelset,
                                EntityPropertyView& view) const;

    PropertyContainerMap::iterator begin() { return _map.begin(); }
    PropertyContainerMap::const_iterator begin() const { return _map.begin(); }
    PropertyContainerMap::iterator end() { return _map.end(); }
    PropertyContainerMap::const_iterator end() const { return _map.end(); }
    PropertyContainerMap::const_iterator find(PropertyTypeID ptID) const { return _map.find(ptID); }
    PropertyContainerMap::iterator find(PropertyTypeID ptID) { return _map.find(ptID); }

    size_t count(PropertyTypeID ptID) const {
        return _map.at(ptID)->size();
    }

    size_t propTypeCount() const {
        return _map.size();
    }

    bool isEmpty() const {
        return _map.empty();
    }

    const PropertyIndexer& indexers() const {
        return _indexers;
    }

    std::span<const EntityID> ids(PropertyTypeID ptID) const {
        return _map.at(ptID)->ids();
    }

    LabelSetPropertyIndexer& addIndexer(PropertyTypeID ptID) {
        return _indexers.emplace(ptID, LabelSetPropertyIndexer {}).first->second;
    }

    LabelSetPropertyIndexer& getIndexer(PropertyTypeID ptID) {
        return _indexers.at(ptID);
    }

    const LabelSetPropertyIndexer& getIndexer(PropertyTypeID ptID) const {
        return _indexers.at(ptID);
    }

    const LabelSetPropertyIndexer* tryGetIndexer(PropertyTypeID ptID) const;

private:
    friend DataPartLoader;
    friend DataPartRebaser;

    PropertyContainerMap _map;

    PropertyContainerReferences _uint64s;
    PropertyContainerReferences _int64s;
    PropertyContainerReferences _doubles;
    PropertyContainerReferences _strings;
    PropertyContainerReferences _bools;

    PropertyIndexer _indexers;
};

}

