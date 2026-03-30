#pragma once

#include <type_traits>
#include <unordered_map>

#include "Index.h"

#include "columns/ColumnIndices.h"
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

    // Unbounded queries
    void query(const Column* input, Column* result) const final;
    void query(const ColumnVector<PropertyPrimitive>* input, ColumnVector<I>* result) const;
    void query(const ColumnConst<PropertyPrimitive>* input, ColumnVector<I>* result) const;

    // Bounded queries
    /**
     * @brief Overload of parent type @ref Index.Returns at most @param limit rows in
     * @param result, updating @param state in-place.
     */
    void boundedQuery(const Column* input,
                      Column* result,
                      ColumnIndices* indices,
                      Index::QueryState& state,
                      size_t limit) const final;

    /// Local specialisation for ColumnVectors
    void boundedQuery(const ColumnVector<PropertyPrimitive>* input,
                      ColumnVector<I>* result,
                      ColumnIndices* indices,
                      Index::QueryState& state,
                      size_t limit = 0) const;

    PropertyTypeID property() const final { return _propID; }

    size_t size() const final;

    bool isNodeIndex() const final { return _isNode; }

private:
    PropertyTypeID _propID;
    PropertyHashMap<PropertyPrimitive, IDContainer, MapType> _hashTable;

    /// Valid since we are constrained by TypedInteralID
    static constexpr bool _isNode = std::is_same_v<I, NodeID>;
};

// Implementation of the underlying hashtable used for retrieval
template <typename K, typename V, typename HashMap>
class PropertyHashMap {
public:
    V& operator[](const K& key) { return _hashMap[key]; }

    size_t size() const { return _hashMap.size(); }

    auto begin() const { return _hashMap.begin(); }
    auto end() const { return _hashMap.end(); }

    bool contains(const K& key) const { return _hashMap.contains(key); }

    auto find(const K& key) const { return _hashMap.find(key); }

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
