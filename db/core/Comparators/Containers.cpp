#include "Comparator.h"
#include "Edge.h"
#include "EdgeMap.h"
#include "Network.h"
#include "Node.h"
#include "PropertyType.h"
#include "Value.h"

namespace db {

// StringMappedContainers
using PropertyTypes = std::map<StringRef, PropertyType*>;
using Networks = std::map<StringRef, Network*>;
using NodeTypes = std::map<StringRef, NodeType*>;
using EdgeTypes = std::map<StringRef, EdgeType*>;

using Properties = std::map<const PropertyType*, Value, DBObject::ObjectComparator>;

// VectorContainers
using Nodes = std::vector<Node*>;
using Edges = std::vector<Edge*>;
using NodeTypeSet = std::set<NodeType*, DBObject::ObjectComparator>;
using EdgeTypeSet = std::set<EdgeType*, DBObject::ObjectComparator>;

template <typename T, typename U>
    requires std::same_as<T, std::map<StringRef, U*>>
static inline bool compareStringMappedContainers(const T* c1, const T* c2) {
    if (c1->size() != c2->size()) {
        return false;
    }

    auto it1 = c1->cbegin();
    auto it2 = c2->cbegin();

    while (it1 != c1->cend()) {
        if (it1->first != it2->first) {
            return false;
        }

        if (!Comparator<U>::same(it1->second, it2->second)) {
            [[maybe_unused]] auto it1first = it1->first;
            [[maybe_unused]] auto it1second = it1->second;
            [[maybe_unused]] auto it2first = it2->first;
            [[maybe_unused]] auto it2second = it2->second;
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

template <typename T, typename U>
    requires std::same_as<T, std::vector<U*>>
          || std::same_as<T, std::set<U*, DBObject::ObjectComparator>>
static inline bool compareVectorContainers(const T* c1, const T* c2) {
    if (c1->size() != c2->size()) {
        return false;
    }

    auto it1 = c1->cbegin();
    auto it2 = c2->cbegin();

    while (it1 != c1->cend()) {
        if (!Comparator<U>::same(*it1, *it2)) {
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

template <>
bool Comparator<PropertyTypes>::same(const PropertyTypes* propTypes1,
                                     const PropertyTypes* propTypes2) {
    return compareStringMappedContainers<PropertyTypes, PropertyType>(
        propTypes1, propTypes2);
}

template <>
bool Comparator<Networks>::same(const Networks* nets1,
                                const Networks* nets2) {
    return compareStringMappedContainers<Networks, Network>(nets1, nets2);
}

template <>
bool Comparator<NodeTypes>::same(const NodeTypes* nt1,
                                 const NodeTypes* nt2) {
    return compareStringMappedContainers<NodeTypes, NodeType>(nt1, nt2);
}

template <>
bool Comparator<EdgeTypes>::same(const EdgeTypes* et1,
                                 const EdgeTypes* et2) {
    return compareStringMappedContainers<EdgeTypes, EdgeType>(et1, et2);
}

template <>
bool Comparator<Nodes>::same(const Nodes* nodes1,
                             const Nodes* nodes2) {
    return compareVectorContainers<Nodes, Node>(nodes1, nodes2);
}

template <>
bool Comparator<Edges>::same(const Edges* edges1, const Edges* edges2) {
    return compareVectorContainers<Edges, Edge>(edges1, edges2);
}

template <>
bool Comparator<NodeTypeSet>::same(const NodeTypeSet* nt1,
                                   const NodeTypeSet* nt2) {
    return compareVectorContainers<NodeTypeSet, NodeType>(nt1, nt2);
}

template <>
bool Comparator<EdgeTypeSet>::same(const EdgeTypeSet* et1,
                                   const EdgeTypeSet* et2) {
    return compareVectorContainers<EdgeTypeSet, EdgeType>(et1, et2);
}

template <>
bool Comparator<Properties>::same(const Properties* props1,
                                  const Properties* props2) {
    if (props1->size() != props2->size()) {
        return false;
    }

    auto it1 = props1->cbegin();
    auto it2 = props2->cbegin();

    while (it1 != props1->cend()) {
        if (!Comparator<PropertyType>::same(it1->first, it2->first)) {
            return false;
        }

        if (!Comparator<Value>::same(&it1->second, &it2->second)) {
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

template <>
bool Comparator<EdgeMap>::same(const EdgeMap* edges1, const EdgeMap* edges2) {
    auto inIterator1 = edges1->_edges.cbegin();
    auto inIterator2 = edges2->_edges.cbegin();

    while (inIterator1 != edges1->_edges.cend()) {
        if (!Comparator<Edges>::same(&inIterator1->second, &inIterator2->second)) {
            return false;
        }

        ++inIterator1;
        ++inIterator2;
    }

    return true;
}

}
