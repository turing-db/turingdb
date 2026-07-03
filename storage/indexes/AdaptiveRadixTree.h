#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string_view>
#include <type_traits>
#include <vector>

namespace db {

/*
 * @brief Type-erased engine behind AdaptiveRadixTree<T>.
 * @detail Holds the whole tree and implements the read/write paths described in docs/ART.md
 * (SIMD child search, leaf fingerprint miss-reject, full-leaf verify, AMAC batched probing). 
 * The value is treated as an opaque blob of `valueSize` bytes so that every node structure, descent and
 * SIMD routine lives in AdaptiveRadixTree.cpp rather than in this header. Use AdaptiveRadixTree<T>;
 * this class is an implementation detail.
 */
class AdaptiveRadixTreeBase {
public:
    AdaptiveRadixTreeBase(size_t valueSize, size_t valueAlignment);
    ~AdaptiveRadixTreeBase();

    AdaptiveRadixTreeBase(const AdaptiveRadixTreeBase&) = delete;
    AdaptiveRadixTreeBase& operator=(const AdaptiveRadixTreeBase&) = delete;

    // Inserts `key` or overwrites its value. Returns true if the key was new, false if it existed.
    bool insert(std::string_view key, const void* value);

    // On a hit, copies the stored value into `value` and returns true; on a miss returns false.
    bool find(std::string_view key, void* value) const;

    bool contains(std::string_view key) const;

    // AMAC-style batched probe: for each of `count` keys, sets found[i] (0/1) and, on a hit, copies
    // the value into the i-th slot of the `values` array (each slot is valueSize bytes).
    void findBatch(const std::string_view* keys,
                   size_t count,
                   uint8_t* found,
                   void* values) const;

    size_t getSize() const { return _size; }
    bool isEmpty() const { return _size == 0; }

private:
    void* _root {nullptr};
    void* _arena {nullptr};   // owns every node and leaf; an opaque BumpPtrAllocator (see the .cpp)
    size_t _size {0};
    size_t _valueSize {0};
    size_t _valueAlignment {0};
};

/*
 * @brief In-memory Adaptive Radix Tree mapping byte-string keys to values of type T.
 * @detail Implements the string-property index read path specified in docs/ART.md: adaptive
 * Node4/16/48/256 inner nodes with path compression and lazy expansion, SIMD child search, a 16-bit
 * leaf-key fingerprint carried in each leaf child pointer for miss-reject, and a full-key verify at
 * the leaf on every lookup. A single-key lookup uses the serial descent; batch-probe sites should use
 * findBatch, which runs an 8-way AMAC-pipelined descent to hide dependent-load latency.
 *
 * This is a single-version, mutable tree. The copy-on-write / MVCC versioning of docs/ART.md §5
 * (retained roots, structural sharing, lock-free readers) layers on top of these node structures and
 * is out of scope here; concurrent reads during a write are not safe.
 *
 * Keys are arbitrary byte strings with one restriction: they must not contain a 0x00 byte, which is
 * reserved as the internal end-of-key marker that keeps the key set prefix-free (so that "foo" and
 * "foobar" can both be stored). String property values satisfy this.
 *
 * The value type must be trivially copyable: values are stored and returned as raw bytes, never
 * constructed, copied or destroyed through T's special members. This fits the intended payloads
 * (entity/row references and similar handles).
 */
template <typename T>
class AdaptiveRadixTree {
public:
    static_assert(std::is_trivially_copyable_v<T>,
                  "AdaptiveRadixTree value type must be trivially copyable; it is stored as raw bytes.");

    AdaptiveRadixTree()
        : _base(sizeof(T), alignof(T))
    {
    }

    AdaptiveRadixTree(const AdaptiveRadixTree&) = delete;
    AdaptiveRadixTree& operator=(const AdaptiveRadixTree&) = delete;

    bool insert(std::string_view key, const T& value) { return _base.insert(key, &value); }

    bool find(std::string_view key, T& value) const { return _base.find(key, &value); }

    bool contains(std::string_view key) const { return _base.contains(key); }

    // Batched point lookup. `found` and `values` are resized to keys.size(); for each key i,
    // found[i] is 1 on a hit (with values[i] filled) and 0 on a miss.
    void findBatch(const std::vector<std::string_view>& keys,
                   std::vector<uint8_t>& found,
                   std::vector<T>& values) const {
        found.resize(keys.size());
        values.resize(keys.size());
        _base.findBatch(keys.data(), keys.size(), found.data(), values.data());
    }

    size_t getSize() const { return _base.getSize(); }
    bool isEmpty() const { return _base.isEmpty(); }

private:
    AdaptiveRadixTreeBase _base;
};

}
