#pragma once

#include <type_traits>
#include <unordered_map>

#include "Index.h"

#include "columns/ColumnVector.h"

#include "ID.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"

namespace db {

template <typename T>
class ColumnVector;

template <typename T>
class ColumnConst;

template <typename K, typename V, typename Map>
class PropertyHashMap;

template <typename K, typename V>
struct PropertyHashMapImpl;

template <SupportedType P, TypedInternalID I>
class PropertyHashIndex final : public Index {
public:
    using IDContainer = ColumnVector<I>;
    using PropertyPrimitive = P::Primitive;
    using MapType = PropertyHashMapImpl<PropertyPrimitive, IDContainer>::type;

    PropertyHashIndex(std::string_view name, PropertyTypeID propertyID);

    void init(GraphView view) final;

    void query(const Column* query, Column* result) final;

private:
    PropertyTypeID _propID;
    PropertyHashMap<PropertyPrimitive, IDContainer, MapType> _hashTable;

    IDContainer _empty {};
    static constexpr bool isNode = std::is_same_v<I, NodeID>;
};

// Implementation of the underlying hashtable used for retrieval
template <typename K, typename V, typename HashMap>
class PropertyHashMap {
public:
    V& operator[](const K& key) { return _hashMap[key]; }

private:
    HashMap _hashMap;
};

// Generic implementation
template <typename K, typename V>
struct PropertyHashMapImpl {
    using type = std::unordered_map<K, V>;
};

// Specialisation to which we pass a custom equality function for embeddings
template <typename V>
struct PropertyHashMapImpl<types::Embedding::Primitive, V> {
    using type = std::unordered_map<types::Embedding::Primitive, V,
                                    std::hash<types::Embedding::Primitive>,
                                    EmbeddingEqual>;
};

}
