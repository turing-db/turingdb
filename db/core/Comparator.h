#pragma once

#include <concepts>

namespace db {

// BaseClasses
class DBObject;
class DBType;
class DBEntityType;
class DBEntity;

// MainObjects
class DB;
class Network;
class Value;
class Property;

// Types
class NodeType;
class EdgeType;
class ValueType;
class PropertyType;

// Entities
class Node;
class Edge;

template <typename T>
concept IterableType = requires(T v) {
    v.cbegin();
    v.cend();
};

template <typename T>
concept ComparableType = requires(T v) {
    // Base classes
       std::same_as<T, DBObject>
    || std::same_as<T, DBType>
    || std::same_as<T, DBEntityType>
    || std::same_as<T, DBEntity>

    // Main objects
    || std::same_as<T, DB>
    || std::same_as<T, Network>
    || std::same_as<T, Value>
    || std::same_as<T, Property>

    // Types
    || std::same_as<T, NodeType>
    || std::same_as<T, EdgeType>
    || std::same_as<T, ValueType>
    || std::same_as<T, PropertyType>

    // Entities
    || std::same_as<T, Node>
    || std::same_as<T, Edge>;
};

template <typename T>
    requires IterableType<T> || ComparableType<T>
struct Comparator {
    static bool same(const T* obj1, const T* obj2);
};

} // namespace db
