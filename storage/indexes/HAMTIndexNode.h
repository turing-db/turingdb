#pragma once

#include <bit>
#include <vector>

#include <stdint.h>

#include "ArcManager.h"

namespace db {

class Column;

template <typename K, typename V, typename Hash>
class HAMTIndex;

class HAMTIndexNode {
public:
    enum class Kind : uint8_t {
        INNER,
        LEAF,

        _SIZE,
    };

    virtual ~HAMTIndexNode() = default;

    virtual constexpr Kind getKind() const = 0;

    template <typename T>
    const T* as() const {
        return dynamic_cast<const T*>(this);
    }

    template <typename T>
    T* as() {
        return dynamic_cast<T*>(this);
    }
};

class HAMTInnerNode final : public HAMTIndexNode {
public:
    template <typename K, typename V, typename Hash>
    friend class HAMTIndex;

    using ChildBitmask = uint16_t;
    using Children = std::vector<WeakArc<HAMTIndexNode>>;

    ChildBitmask mask() const { return _mask; }
    const Children& children() const { return _children; }

    void insertChild(size_t hashChunk, WeakArc<HAMTIndexNode>& child);

    constexpr Kind getKind() const final { return _kind; }

private:
    constexpr static HAMTIndexNode::Kind _kind {HAMTIndexNode::Kind::INNER};

    ChildBitmask _mask {0};
    Children _children;
};

template <typename K, typename V>
class HAMTLeaf final : public HAMTIndexNode {
public:
    template <typename T, typename U, typename Hash>
    friend class HAMTIndex;

    using KVPair = std::pair<K, V>;
    using Pairs = std::vector<KVPair>;

    const Pairs& values() const { return _values; }

    template <typename... Args>
    void emplace_back(Args... args) {
        _values.emplace_back(args...);
    }

    constexpr Kind getKind() const final { return _kind; }

private:
    constexpr static HAMTIndexNode::Kind _kind {HAMTIndexNode::Kind::LEAF};

    Pairs _values; 
};

}
