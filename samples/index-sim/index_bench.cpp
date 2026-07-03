// index_bench — wall-clock microbenchmark of three versioned string-key index designs, to
// validate (or correct) the fixed-cost estimates in index_sim.cpp with real timings:
//
//   1. Multiversion hash   — one global open-addressed table; each slot holds a newest-first
//                            version chain. HEAD read = probe + head; as-of read = walk the chain.
//   2. HAMT                — persistent hash-array-mapped trie, 32-way bitmap-compressed nodes,
//                            copy-on-write path-copy per write, a retained root per commit.
//   3. ART                 — persistent adaptive radix tree (Node4/16/48/256) with pessimistic
//                            path compression; copy-on-write; a retained root per commit. No hashing.
//
// All three index the SAME workload (replayed from one generated write sequence) over a SHARED key
// pool, so the only thing that differs is the structure. Keys are random fixed-length strings; the
// value written for key k at commit c is encode(c, k), so a HEAD read must return the last writer
// and an as-of read at version T must return the latest writer <= T. Correctness is cross-checked
// across all three before any timing.
//
// It measures, in real nanoseconds: HEAD point-read latency, submit (apply one commit) latency, and
// as-of (time-travel) read latency — the last via a hot key written in every commit, the case the
// cost model flagged as the multiversion hash's weakness. Retained index memory is measured by
// walking the live structure (trie: reachable nodes from every retained root, deduplicated).
//
// Build:  g++ -std=c++23 -O2 -o index_bench index_bench.cpp
// Run:    ./index_bench
//         ./index_bench --key-len 100        # long keys — stresses the hash's per-byte digest
//         ./index_bench --keys 200000 --parts 2000 --writes 32 --reads 3000000
//         ./index_bench --branch 64          # HAMT fan-out (5 bits default; 6 bits = 64-way)

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <string_view>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <cstdio>

namespace {

// ---- workload / cost parameters -------------------------------------------------------------

struct BenchParams {
    size_t keys {100000};        // distinct string keys (cardinality)
    size_t writesPerCommit {32};
    size_t parts {2000};         // commits / retained versions
    size_t keyLenBytes {16};
    size_t reads {2000000};      // HEAD lookups timed
    size_t asOfReads {2000000};  // as-of lookups timed
    size_t hamtBits {5};         // HAMT bits/level (5 => 32-way, 6 => 64-way)
    size_t churnedKeys {1024};   // size of the low-cardinality churned key SET (read cache-realistically)
    size_t churnedWrites {0};    // churned writes per commit (0 => half the commit, minus the hot key)
    uint64_t seed {0x9E3779B97F4A7C15ull};
};

struct SplitMix64 {
    uint64_t _state {0};
    explicit SplitMix64(uint64_t seed) : _state(seed) {}
    uint64_t next() {
        uint64_t z = (_state += 0x9E3779B97F4A7C15ull);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        return z ^ (z >> 31);
    }
    size_t nextIndex(size_t bound) { return static_cast<size_t>(next() % bound); }
};

// FNV-1a 64, used by BOTH the HAMT and the multiversion hash so neither is advantaged by the hash.
uint64_t fnv1a(const char* data, size_t len) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < len; ++i) {
        h ^= static_cast<uint8_t>(data[i]);
        h *= 1099511628211ull;
    }
    return h;
}
uint64_t fnv1a(std::string_view s) { return fnv1a(s.data(), s.size()); }

uint64_t encodeValue(size_t commit, size_t key) {
    return (static_cast<uint64_t>(commit) << 24) | static_cast<uint64_t>(key);
}

double nowNs() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration<double, std::nano>(clock::now().time_since_epoch()).count();
}

// ---- 1. Multiversion hash -------------------------------------------------------------------
// Global open-addressed table (power-of-two, linear probe). Each used slot stores the key and a
// newest-first singly linked version chain. Insert prepends a version node; HEAD read returns the
// head; as-of read walks past versions newer than the snapshot.

struct VersionNode {
    uint32_t version {0};
    uint64_t value {0};
    VersionNode* older {nullptr};
};

struct MvccSlot {
    std::string_view key;
    uint64_t hash {0};
    VersionNode* head {nullptr};
    bool used {false};
};

class MvccHash {
public:
    explicit MvccHash(size_t expectedKeys) {
        size_t cap = 16;
        while (cap < expectedKeys * 2) {
            cap <<= 1;
        }
        _slots.assign(cap, MvccSlot{});
        _mask = cap - 1;
    }

    void insert(std::string_view key, uint64_t hash, uint64_t value, uint32_t version) {
        if ((_count + 1) * 10 >= _slots.size() * 7) {
            grow();
        }
        MvccSlot& slot = locate(key, hash);
        if (!slot.used) {
            slot.used = true;
            slot.key = key;
            slot.hash = hash;
            ++_count;
        }
        VersionNode* node = new VersionNode{version, value, slot.head};
        _versionBytes += sizeof(VersionNode);
        slot.head = node;
    }

    bool lookupHead(std::string_view key, uint64_t hash, uint64_t& out) const {
        const MvccSlot& slot = locate(key, hash);
        if (!slot.used || slot.head == nullptr) {
            return false;
        }
        out = slot.head->value;
        return true;
    }

    bool lookupAsOf(std::string_view key, uint64_t hash, uint32_t version, uint64_t& out) const {
        const MvccSlot& slot = locate(key, hash);
        if (!slot.used) {
            return false;
        }
        for (const VersionNode* node = slot.head; node != nullptr; node = node->older) {
            if (node->version <= version) {
                out = node->value;
                return true;
            }
        }
        return false;
    }

    size_t retainedBytes() const {
        return _slots.size() * sizeof(MvccSlot) + _versionBytes;
    }

private:
    const MvccSlot& locate(std::string_view key, uint64_t hash) const {
        size_t i = hash & _mask;
        while (_slots[i].used && !(_slots[i].hash == hash && _slots[i].key == key)) {
            i = (i + 1) & _mask;
        }
        return _slots[i];
    }
    MvccSlot& locate(std::string_view key, uint64_t hash) {
        const MvccHash* self = this;
        return const_cast<MvccSlot&>(self->locate(key, hash));
    }

    void grow() {
        std::vector<MvccSlot> old;
        old.swap(_slots);
        _slots.assign(old.size() * 2, MvccSlot{});
        _mask = _slots.size() - 1;
        for (const MvccSlot& slot : old) {
            if (slot.used) {
                size_t i = slot.hash & _mask;
                while (_slots[i].used) {
                    i = (i + 1) & _mask;
                }
                _slots[i] = slot;
            }
        }
    }

    std::vector<MvccSlot> _slots;
    size_t _mask {0};
    size_t _count {0};
    size_t _versionBytes {0};
};

// ---- 2. HAMT --------------------------------------------------------------------------------
// Persistent, bitmap-compressed, configurable bits/level. A child slot is a tagged pointer: a leaf
// (low bit set) or a sub-node. Insert is functional copy-on-write: every node on the path is cloned,
// producing a new root; untouched sub-tries are shared. Threading the root through a commit's writes
// shares the upper path-copies across that commit (the realistic per-commit cost).

struct HamtLeaf {
    std::string_view key;
    uint64_t hash {0};
    uint64_t value {0};
};

struct HamtNode {
    uint32_t bitmap {0};
    uint32_t count {0};
    void** children {nullptr};  // length == count; each entry tagged: leaf | node
};

inline bool isLeaf(void* p) { return (reinterpret_cast<uintptr_t>(p) & 1u) != 0; }
inline void* tagLeaf(HamtLeaf* l) { return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(l) | 1u); }
inline HamtLeaf* asLeaf(void* p) { return reinterpret_cast<HamtLeaf*>(reinterpret_cast<uintptr_t>(p) & ~uintptr_t(1)); }
inline HamtNode* asNode(void* p) { return reinterpret_cast<HamtNode*>(p); }

class Hamt {
public:
    explicit Hamt(size_t bitsPerLevel)
        : _bits(bitsPerLevel),
          _mask((1u << bitsPerLevel) - 1u),
          _maxDepth((64 + bitsPerLevel - 1) / bitsPerLevel) {}

    // Apply one write to a root, returning the new root (copy-on-write).
    void* insert(void* root, std::string_view key, uint64_t hash, uint64_t value) {
        return insertRec(root, key, hash, value, 0);
    }

    bool lookup(void* root, std::string_view key, uint64_t hash, uint64_t& out) const {
        void* cur = root;
        size_t depth = 0;
        while (cur != nullptr) {
            if (isLeaf(cur)) {
                HamtLeaf* leaf = asLeaf(cur);
                if (leaf->hash == hash && leaf->key == key) {
                    out = leaf->value;
                    return true;
                }
                return false;
            }
            HamtNode* node = asNode(cur);
            const uint32_t slot = sliceAt(hash, depth);
            const uint32_t bit = 1u << slot;
            if ((node->bitmap & bit) == 0) {
                return false;
            }
            cur = node->children[indexOf(node->bitmap, bit)];
            ++depth;
        }
        return false;
    }

    static size_t retainedBytes(const std::vector<void*>& roots) {
        std::unordered_set<const void*> seen;
        size_t bytes = 0;
        for (void* root : roots) {
            visit(root, seen, bytes);
        }
        return bytes;
    }

private:
    static uint32_t indexOf(uint32_t bitmap, uint32_t bit) {
        return static_cast<uint32_t>(__builtin_popcount(bitmap & (bit - 1)));
    }
    uint32_t sliceAt(uint64_t hash, size_t depth) const {
        // Past the hash's bits, fold in the depth so distinct keys still separate (collision tail).
        const size_t shift = depth * _bits;
        if (shift < 64) {
            return static_cast<uint32_t>((hash >> shift) & _mask);
        }
        return static_cast<uint32_t>((hash ^ (hash >> 17) ^ (depth * 0x9E37u)) & _mask);
    }

    HamtLeaf* newLeaf(std::string_view key, uint64_t hash, uint64_t value) {
        return new HamtLeaf{key, hash, value};
    }

    HamtNode* newNode(uint32_t bitmap, uint32_t count) {
        HamtNode* node = new HamtNode{bitmap, count, new void*[count]};
        return node;
    }

    // Build a node holding two children that diverge at `depth` (or deeper).
    void* pairNode(void* a, uint32_t slotA, void* b, uint32_t slotB) {
        if (slotA == slotB) {
            HamtNode* node = newNode(1u << slotA, 1);
            node->children[0] = nullptr;  // filled by caller via re-insert path
            return node;  // unused branch; handled in insertRec split instead
        }
        const uint32_t bitmap = (1u << slotA) | (1u << slotB);
        HamtNode* node = newNode(bitmap, 2);
        if (slotA < slotB) {
            node->children[0] = a;
            node->children[1] = b;
        } else {
            node->children[0] = b;
            node->children[1] = a;
        }
        return node;
    }

    void* insertRec(void* cur, std::string_view key, uint64_t hash, uint64_t value, size_t depth) {
        if (cur == nullptr) {
            return tagLeaf(newLeaf(key, hash, value));
        }
        if (isLeaf(cur)) {
            HamtLeaf* leaf = asLeaf(cur);
            if (leaf->hash == hash && leaf->key == key) {
                return tagLeaf(newLeaf(key, hash, value));  // replace
            }
            // Split: push both leaves down until their slices diverge.
            return splitLeaves(leaf, tagLeaf(newLeaf(key, hash, value)), hash, depth);
        }
        HamtNode* node = asNode(cur);
        const uint32_t slot = sliceAt(hash, depth);
        const uint32_t bit = 1u << slot;
        const uint32_t idx = indexOf(node->bitmap, bit);
        if ((node->bitmap & bit) == 0) {
            // Add a new leaf child: clone node with one extra slot.
            HamtNode* copy = newNode(node->bitmap | bit, node->count + 1);
            for (uint32_t i = 0; i < idx; ++i) {
                copy->children[i] = node->children[i];
            }
            copy->children[idx] = tagLeaf(newLeaf(key, hash, value));
            for (uint32_t i = idx; i < node->count; ++i) {
                copy->children[i + 1] = node->children[i];
            }
            return copy;
        }
        // Recurse into existing child; clone this node with the rebuilt child.
        void* newChild = insertRec(node->children[idx], key, hash, value, depth + 1);
        HamtNode* copy = newNode(node->bitmap, node->count);
        for (uint32_t i = 0; i < node->count; ++i) {
            copy->children[i] = node->children[i];
        }
        copy->children[idx] = newChild;
        return copy;
    }

    // Two leaves (existing + new, both tagged) that collided higher up: nest nodes until they split.
    void* splitLeaves(HamtLeaf* existing, void* newLeafTagged, uint64_t newHash, size_t depth) {
        HamtLeaf* added = asLeaf(newLeafTagged);
        const uint32_t slotE = sliceAt(existing->hash, depth);
        const uint32_t slotN = sliceAt(added->hash, depth);
        if (slotE != slotN) {
            return pairNode(tagLeaf(existing), slotE, newLeafTagged, slotN);
        }
        if (depth + 1 >= _maxDepth + 16) {
            // Astronomically rare full collision: chain via a single-slot node (still correct via key compare).
            HamtNode* node = newNode(1u << slotE, 1);
            node->children[0] = tagLeaf(existing);
            // Overwrite-on-equal already handled; distinct keys with identical 64-bit hash land here.
            // Store the new leaf by re-pairing one level deeper using a perturbed slice.
            node->children[0] = splitLeaves(existing, newLeafTagged, newHash, depth + 1);
            return node;
        }
        void* deeper = splitLeaves(existing, newLeafTagged, newHash, depth + 1);
        HamtNode* node = newNode(1u << slotE, 1);
        node->children[0] = deeper;
        return node;
    }

    static void visit(void* cur, std::unordered_set<const void*>& seen, size_t& bytes) {
        if (cur == nullptr) {
            return;
        }
        if (isLeaf(cur)) {
            const HamtLeaf* leaf = asLeaf(cur);
            if (seen.insert(leaf).second) {
                bytes += sizeof(HamtLeaf);
            }
            return;
        }
        const HamtNode* node = asNode(cur);
        if (!seen.insert(node).second) {
            return;
        }
        bytes += sizeof(HamtNode) + node->count * sizeof(void*);
        for (uint32_t i = 0; i < node->count; ++i) {
            visit(node->children[i], seen, bytes);
        }
    }

    size_t _bits;
    uint32_t _mask;
    size_t _maxDepth;
};

// ---- 3. ART (Adaptive Radix Tree) -----------------------------------------------------------
// Persistent, copy-on-write. Adaptive node types Node4/16/48/256 keep sparse nodes small. Pessimistic
// path compression: each inner node stores the bytes shared by all its descendants since the parent's
// branch. A leaf stores the full key. No hashing — descent is by key bytes.

enum class ArtType : uint8_t { Leaf, N4, N16, N48, N256 };
static const size_t ART_MAX_PREFIX = 16;  // random keys diverge in ~1 byte, so this is never exceeded

struct ArtBase {
    ArtType type;
};

struct ArtLeaf : ArtBase {
    std::string_view key;
    uint64_t value {0};
};

struct ArtInner : ArtBase {
    uint16_t numChildren {0};
    uint8_t prefixLen {0};
    uint8_t prefix[ART_MAX_PREFIX];
};

struct ArtN4 : ArtInner {
    uint8_t keys[4];
    ArtBase* child[4];
};
struct ArtN16 : ArtInner {
    uint8_t keys[16];
    ArtBase* child[16];
};
struct ArtN48 : ArtInner {
    uint8_t childIndex[256];   // byte -> slot+1 (0 = empty)
    ArtBase* child[48];
};
struct ArtN256 : ArtInner {
    ArtBase* child[256];
};

class Art {
public:
    ArtBase* insert(ArtBase* root, std::string_view key, uint64_t value) {
        return insertRec(root, key, value, 0);
    }

    bool lookup(ArtBase* root, std::string_view key, uint64_t& out) const {
        ArtBase* cur = root;
        size_t depth = 0;
        while (cur != nullptr) {
            if (cur->type == ArtType::Leaf) {
                ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
                if (leaf->key == key) {
                    out = leaf->value;
                    return true;
                }
                return false;
            }
            ArtInner* node = static_cast<ArtInner*>(cur);
            if (node->prefixLen > 0) {
                if (depth + node->prefixLen > key.size()) {
                    return false;
                }
                if (memcmp(node->prefix, key.data() + depth, node->prefixLen) != 0) {
                    return false;
                }
                depth += node->prefixLen;
            }
            if (depth >= key.size()) {
                return false;
            }
            ArtBase** slot = findChild(node, static_cast<uint8_t>(key[depth]));
            if (slot == nullptr || *slot == nullptr) {
                return false;
            }
            cur = *slot;
            ++depth;
        }
        return false;
    }

    static size_t retainedBytes(const std::vector<ArtBase*>& roots) {
        std::unordered_set<const void*> seen;
        size_t bytes = 0;
        for (ArtBase* root : roots) {
            visit(root, seen, bytes);
        }
        return bytes;
    }

private:
    ArtLeaf* newLeaf(std::string_view key, uint64_t value) {
        ArtLeaf* leaf = new ArtLeaf();
        leaf->type = ArtType::Leaf;
        leaf->key = key;
        leaf->value = value;
        return leaf;
    }

    ArtN4* newN4() {
        ArtN4* node = new ArtN4();
        node->type = ArtType::N4;
        node->numChildren = 0;
        node->prefixLen = 0;
        memset(node->keys, 0, sizeof(node->keys));
        memset(node->child, 0, sizeof(node->child));
        return node;
    }

    static ArtBase** findChild(ArtInner* node, uint8_t byte) {
        switch (node->type) {
            case ArtType::N4: {
                ArtN4* n = static_cast<ArtN4*>(node);
                for (uint16_t i = 0; i < n->numChildren; ++i) {
                    if (n->keys[i] == byte) {
                        return &n->child[i];
                    }
                }
                return nullptr;
            }
            case ArtType::N16: {
                ArtN16* n = static_cast<ArtN16*>(node);
                for (uint16_t i = 0; i < n->numChildren; ++i) {
                    if (n->keys[i] == byte) {
                        return &n->child[i];
                    }
                }
                return nullptr;
            }
            case ArtType::N48: {
                ArtN48* n = static_cast<ArtN48*>(node);
                const uint8_t pos = n->childIndex[byte];
                if (pos == 0) {
                    return nullptr;
                }
                return &n->child[pos - 1];
            }
            case ArtType::N256: {
                ArtN256* n = static_cast<ArtN256*>(node);
                if (n->child[byte] == nullptr) {
                    return nullptr;
                }
                return &n->child[byte];
            }
            default:
                return nullptr;
                break;
        }
    }

    // Deep-clone one inner node (its own arrays), sharing the child sub-tries.
    ArtInner* cloneInner(ArtInner* node) {
        switch (node->type) {
            case ArtType::N4: {
                ArtN4* n = new ArtN4(*static_cast<ArtN4*>(node));
                return n;
            }
            case ArtType::N16: {
                ArtN16* n = new ArtN16(*static_cast<ArtN16*>(node));
                return n;
            }
            case ArtType::N48: {
                ArtN48* n = new ArtN48(*static_cast<ArtN48*>(node));
                return n;
            }
            case ArtType::N256: {
                ArtN256* n = new ArtN256(*static_cast<ArtN256*>(node));
                return n;
            }
            default:
                return nullptr;
                break;
        }
    }

    // Add a child to a (freshly cloned/owned) inner node, growing the node type if full. Returns the
    // node to use (same pointer, or a grown replacement).
    ArtInner* addChild(ArtInner* node, uint8_t byte, ArtBase* childPtr) {
        switch (node->type) {
            case ArtType::N4: {
                ArtN4* n = static_cast<ArtN4*>(node);
                if (n->numChildren < 4) {
                    n->keys[n->numChildren] = byte;
                    n->child[n->numChildren] = childPtr;
                    ++n->numChildren;
                    return n;
                }
                ArtN16* grown = new ArtN16();
                grown->type = ArtType::N16;
                copyHeader(grown, n);
                for (uint16_t i = 0; i < 4; ++i) {
                    grown->keys[i] = n->keys[i];
                    grown->child[i] = n->child[i];
                }
                grown->numChildren = 4;
                grown->keys[4] = byte;
                grown->child[4] = childPtr;
                grown->numChildren = 5;
                return grown;
            }
            case ArtType::N16: {
                ArtN16* n = static_cast<ArtN16*>(node);
                if (n->numChildren < 16) {
                    n->keys[n->numChildren] = byte;
                    n->child[n->numChildren] = childPtr;
                    ++n->numChildren;
                    return n;
                }
                ArtN48* grown = new ArtN48();
                grown->type = ArtType::N48;
                copyHeader(grown, n);
                memset(grown->childIndex, 0, sizeof(grown->childIndex));
                memset(grown->child, 0, sizeof(grown->child));
                for (uint16_t i = 0; i < 16; ++i) {
                    grown->child[i] = n->child[i];
                    grown->childIndex[n->keys[i]] = static_cast<uint8_t>(i + 1);
                }
                grown->numChildren = 16;
                grown->child[16] = childPtr;
                grown->childIndex[byte] = 17;
                grown->numChildren = 17;
                return grown;
            }
            case ArtType::N48: {
                ArtN48* n = static_cast<ArtN48*>(node);
                if (n->numChildren < 48) {
                    const uint16_t pos = n->numChildren;
                    n->child[pos] = childPtr;
                    n->childIndex[byte] = static_cast<uint8_t>(pos + 1);
                    ++n->numChildren;
                    return n;
                }
                ArtN256* grown = new ArtN256();
                grown->type = ArtType::N256;
                copyHeader(grown, n);
                memset(grown->child, 0, sizeof(grown->child));
                for (uint16_t b = 0; b < 256; ++b) {
                    const uint8_t pos = n->childIndex[b];
                    if (pos != 0) {
                        grown->child[b] = n->child[pos - 1];
                    }
                }
                grown->numChildren = 48;
                grown->child[byte] = childPtr;
                ++grown->numChildren;
                return grown;
            }
            case ArtType::N256: {
                ArtN256* n = static_cast<ArtN256*>(node);
                n->child[byte] = childPtr;
                ++n->numChildren;
                return n;
            }
            default:
                return node;
                break;
        }
    }

    static void copyHeader(ArtInner* dst, ArtInner* src) {
        dst->numChildren = src->numChildren;
        dst->prefixLen = src->prefixLen;
        memcpy(dst->prefix, src->prefix, ART_MAX_PREFIX);
    }

    size_t firstDifference(std::string_view a, std::string_view b, size_t from) {
        const size_t n = std::min(a.size(), b.size());
        size_t i = from;
        while (i < n && a[i] == b[i]) {
            ++i;
        }
        return i;
    }

    ArtBase* insertRec(ArtBase* cur, std::string_view key, uint64_t value, size_t depth) {
        if (cur == nullptr) {
            return newLeaf(key, value);
        }
        if (cur->type == ArtType::Leaf) {
            ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
            if (leaf->key == key) {
                return newLeaf(key, value);  // replace
            }
            // Split two leaves: build a Node4 at their first differing byte, with a shared prefix.
            const size_t diff = firstDifference(leaf->key, key, depth);
            ArtN4* node = newN4();
            node->prefixLen = static_cast<uint8_t>(std::min(diff - depth, ART_MAX_PREFIX));
            memcpy(node->prefix, key.data() + depth, node->prefixLen);
            ArtInner* filled = addChild(node, static_cast<uint8_t>(leaf->key[diff]), leaf);
            filled = addChild(filled, static_cast<uint8_t>(key[diff]), newLeaf(key, value));
            return filled;
        }
        ArtInner* node = static_cast<ArtInner*>(cur);
        // Match the stored prefix; on mismatch, split this node at the mismatch point.
        if (node->prefixLen > 0) {
            size_t p = 0;
            while (p < node->prefixLen && depth + p < key.size()
                   && node->prefix[p] == static_cast<uint8_t>(key[depth + p])) {
                ++p;
            }
            if (p != node->prefixLen) {
                // Prefix diverges: new Node4 holding [old node (shortened prefix)] and [new leaf].
                ArtN4* split = newN4();
                split->prefixLen = static_cast<uint8_t>(p);
                memcpy(split->prefix, node->prefix, p);

                ArtInner* shifted = cloneInner(node);
                const uint8_t oldByte = node->prefix[p];
                shifted->prefixLen = static_cast<uint8_t>(node->prefixLen - p - 1);
                memmove(shifted->prefix, node->prefix + p + 1, shifted->prefixLen);

                ArtInner* filled = addChild(split, oldByte, shifted);
                filled = addChild(filled, static_cast<uint8_t>(key[depth + p]), newLeaf(key, value));
                return filled;
            }
            depth += node->prefixLen;
        }
        const uint8_t byte = static_cast<uint8_t>(key[depth]);
        ArtBase** slot = findChild(node, byte);
        if (slot != nullptr) {
            ArtBase* newChild = insertRec(*slot, key, value, depth + 1);
            ArtInner* copy = cloneInner(node);
            *findChild(copy, byte) = newChild;
            return copy;
        }
        // No child for this byte: clone, then add (which may grow the node type).
        ArtInner* copy = cloneInner(node);
        return addChild(copy, byte, newLeaf(key, value));
    }

    static size_t nodeBytes(const ArtBase* n) {
        switch (n->type) {
            case ArtType::Leaf: return sizeof(ArtLeaf); break;
            case ArtType::N4: return sizeof(ArtN4); break;
            case ArtType::N16: return sizeof(ArtN16); break;
            case ArtType::N48: return sizeof(ArtN48); break;
            case ArtType::N256: return sizeof(ArtN256); break;
            default: return 0; break;
        }
    }

    static void visitChild(ArtBase* c, std::unordered_set<const void*>& seen, size_t& bytes) {
        if (c != nullptr) {
            visit(c, seen, bytes);
        }
    }

    static void visit(ArtBase* cur, std::unordered_set<const void*>& seen, size_t& bytes) {
        if (cur == nullptr || !seen.insert(cur).second) {
            return;
        }
        bytes += nodeBytes(cur);
        switch (cur->type) {
            case ArtType::Leaf:
                break;
            case ArtType::N4: {
                ArtN4* n = static_cast<ArtN4*>(cur);
                for (uint16_t i = 0; i < n->numChildren; ++i) { visitChild(n->child[i], seen, bytes); }
                break;
            }
            case ArtType::N16: {
                ArtN16* n = static_cast<ArtN16*>(cur);
                for (uint16_t i = 0; i < n->numChildren; ++i) { visitChild(n->child[i], seen, bytes); }
                break;
            }
            case ArtType::N48: {
                ArtN48* n = static_cast<ArtN48*>(cur);
                for (uint16_t i = 0; i < 48; ++i) { visitChild(n->child[i], seen, bytes); }
                break;
            }
            case ArtType::N256: {
                ArtN256* n = static_cast<ArtN256*>(cur);
                for (uint16_t i = 0; i < 256; ++i) { visitChild(n->child[i], seen, bytes); }
                break;
            }
            default:
                break;
        }
    }
};

// ---- workload generation --------------------------------------------------------------------
// One write sequence, replayed by all three structures. A "hot" key (index 0) is written in every
// commit so its version chain reaches full depth — the time-travel stress case.

struct Workload {
    std::vector<std::string> keyPool;     // distinct keys (the shared dictionary / StringContainer)
    std::vector<std::string_view> keyView;
    std::vector<uint64_t> keyHash;
    std::vector<uint32_t> writes;         // flat: parts * writesPerCommit key indices
    std::vector<size_t> lastCommit;       // last commit that wrote each key (-> expected HEAD value)
    std::vector<uint32_t> writeCount;     // times each key was written (== its version-chain depth)
    std::vector<uint8_t> present;
    size_t hotKey {0};                    // index 0: written every commit (depth == parts)
    size_t churnedCount {0};              // churned key SET occupies indices [1, churnedCount)
    size_t hotAsOfVersion {1};            // as-of snapshot used for the time-travel benchmark
};

void buildWorkload(const BenchParams& params, Workload& w) {
    SplitMix64 rng(params.seed);
    const char* alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    const size_t alphabetSize = 62;

    w.keyPool.resize(params.keys);
    w.keyView.resize(params.keys);
    w.keyHash.resize(params.keys);
    for (size_t k = 0; k < params.keys; ++k) {
        std::string s(params.keyLenBytes, ' ');
        for (size_t c = 0; c < params.keyLenBytes; ++c) {
            s[c] = alphabet[rng.nextIndex(alphabetSize)];
        }
        // Stamp the index in to guarantee distinctness regardless of length.
        uint64_t tag = k;
        for (size_t c = 0; c < params.keyLenBytes && tag > 0; ++c) {
            s[c] = alphabet[tag % alphabetSize];
            tag /= alphabetSize;
        }
        w.keyPool[k] = s;
    }
    for (size_t k = 0; k < params.keys; ++k) {
        w.keyView[k] = w.keyPool[k];
        w.keyHash[k] = fnv1a(w.keyView[k]);
    }

    // Three key populations:
    //   index 0           — the hot key, written EVERY commit (chain depth == parts); used for the
    //                       single-key compute-only contrast and the as-of correctness check.
    //   [1, churnedCount) — the churned SET: low-cardinality keys rewritten often (deep-ish chains),
    //                       read as a shuffled stream so the measurement is cache-realistic.
    //   [churnedCount, keys) — high-cardinality keys, each written rarely (short chains).
    w.churnedCount = std::min(params.churnedKeys, params.keys);
    const size_t W = params.writesPerCommit;
    size_t churnedW = params.churnedWrites != 0 ? params.churnedWrites : (W > 1 ? (W - 1) / 2 : 0);
    if (churnedW > (W > 0 ? W - 1 : 0)) {
        churnedW = (W > 0 ? W - 1 : 0);
    }

    w.lastCommit.assign(params.keys, static_cast<size_t>(-1));
    w.writeCount.assign(params.keys, 0);
    w.present.assign(params.keys, 0);
    w.writes.resize(params.parts * W);
    for (size_t c = 0; c < params.parts; ++c) {
        for (size_t j = 0; j < W; ++j) {
            size_t idx;
            if (j == 0) {
                idx = w.hotKey;
            } else if (j <= churnedW && w.churnedCount > 1) {
                idx = 1 + rng.nextIndex(w.churnedCount - 1);
            } else if (w.churnedCount < params.keys) {
                idx = w.churnedCount + rng.nextIndex(params.keys - w.churnedCount);
            } else {
                idx = rng.nextIndex(params.keys);
            }
            w.writes[c * W + j] = static_cast<uint32_t>(idx);
            w.present[idx] = 1;
            w.lastCommit[idx] = c;
            ++w.writeCount[idx];
        }
    }
}

// ---- timing helpers -------------------------------------------------------------------------

struct ReadResult {
    double nsPerOp {0.0};
    uint64_t checksum {0};
};

// ---- build / benchmark each structure -------------------------------------------------------

struct BuildTimings {
    double submitNsPerCommit {0.0};
    double buildTotalMs {0.0};
    size_t retainedBytes {0};
};

void run(const BenchParams& params) {
    std::printf("index_bench — real wall-clock timings (g++ -O2)\n");
    std::printf("workload: keys=%zu writes/commit=%zu parts=%zu key-len=%zuB reads=%zu hamt-bits=%zu(%zu-way)\n",
                params.keys, params.writesPerCommit, params.parts, params.keyLenBytes, params.reads,
                params.hamtBits, size_t(1) << params.hamtBits);

    Workload w;
    buildWorkload(params, w);

    size_t distinct = 0;
    for (uint8_t p : w.present) { distinct += p; }
    double churnedDepthSum = 0.0;
    size_t churnedPresent = 0;
    for (size_t k = 1; k < w.churnedCount; ++k) {
        if (w.present[k]) { churnedDepthSum += w.writeCount[k]; ++churnedPresent; }
    }
    const double avgChurnedDepth = churnedPresent ? churnedDepthSum / static_cast<double>(churnedPresent) : 0.0;
    std::printf("distinct keys written: %zu  (hot key #0 every commit; churned set [1,%zu) avg chain depth %.0f)\n\n",
                distinct, w.churnedCount, avgChurnedDepth);

    const size_t W = params.writesPerCommit;

    // --- build MVCC ---
    MvccHash mvcc(params.keys);
    double t0 = nowNs();
    for (size_t c = 0; c < params.parts; ++c) {
        for (size_t j = 0; j < W; ++j) {
            const uint32_t k = w.writes[c * W + j];
            mvcc.insert(w.keyView[k], w.keyHash[k], encodeValue(c, k), static_cast<uint32_t>(c));
        }
    }
    const double mvccBuildNs = nowNs() - t0;

    // --- build HAMT (retain a root per commit) ---
    Hamt hamt(params.hamtBits);
    std::vector<void*> hamtRoots(params.parts, nullptr);
    void* hroot = nullptr;
    t0 = nowNs();
    for (size_t c = 0; c < params.parts; ++c) {
        for (size_t j = 0; j < W; ++j) {
            const uint32_t k = w.writes[c * W + j];
            hroot = hamt.insert(hroot, w.keyView[k], w.keyHash[k], encodeValue(c, k));
        }
        hamtRoots[c] = hroot;
    }
    const double hamtBuildNs = nowNs() - t0;

    // --- build ART (retain a root per commit) ---
    Art art;
    std::vector<ArtBase*> artRoots(params.parts, nullptr);
    ArtBase* aroot = nullptr;
    t0 = nowNs();
    for (size_t c = 0; c < params.parts; ++c) {
        for (size_t j = 0; j < W; ++j) {
            const uint32_t k = w.writes[c * W + j];
            aroot = art.insert(aroot, w.keyView[k], encodeValue(c, k));
        }
        artRoots[c] = aroot;
    }
    const double artBuildNs = nowNs() - t0;

    // --- correctness: all three must agree with the independently computed last-writer at HEAD ---
    size_t checked = 0;
    size_t mismatches = 0;
    for (size_t k = 0; k < params.keys; ++k) {
        if (!w.present[k]) {
            continue;
        }
        const uint64_t expect = encodeValue(w.lastCommit[k], k);
        uint64_t a = 0, b = 0, cc = 0;
        const bool fa = mvcc.lookupHead(w.keyView[k], w.keyHash[k], a);
        const bool fb = hamt.lookup(hroot, w.keyView[k], w.keyHash[k], b);
        const bool fc = art.lookup(aroot, w.keyView[k], cc);
        if (!fa || !fb || !fc || a != expect || b != expect || cc != expect) {
            ++mismatches;
            if (mismatches <= 5) {
                std::printf("  MISMATCH key %zu: expect=%llu mvcc=%llu(%d) hamt=%llu(%d) art=%llu(%d)\n",
                            k, (unsigned long long)expect, (unsigned long long)a, fa,
                            (unsigned long long)b, fb, (unsigned long long)cc, fc);
            }
        }
        ++checked;
    }
    // as-of correctness on the hot key
    const size_t T = std::min(w.hotAsOfVersion, params.parts - 1);
    const uint64_t expectAsOf = encodeValue(T, w.hotKey);
    uint64_t av = 0, bv = 0, cv = 0;
    mvcc.lookupAsOf(w.keyView[w.hotKey], w.keyHash[w.hotKey], static_cast<uint32_t>(T), av);
    hamt.lookup(hamtRoots[T], w.keyView[w.hotKey], w.keyHash[w.hotKey], bv);
    art.lookup(artRoots[T], w.keyView[w.hotKey], cv);
    const bool asOfOk = (av == expectAsOf && bv == expectAsOf && cv == expectAsOf);
    std::printf("correctness: checked %zu present keys, %zu mismatches; as-of(hot,v%zu) %s\n\n",
                checked, mismatches, T, asOfOk ? "OK" : "FAILED");
    if (mismatches != 0 || !asOfOk) {
        std::printf("ABORTING: structures disagree — timings would be meaningless.\n");
        return;
    }

    // --- HEAD read latency: shuffled present keys, multiple passes, report best ns/op ---
    std::vector<uint32_t> probe;
    probe.reserve(params.reads);
    {
        SplitMix64 prng(params.seed ^ 0xABCDEF);
        std::vector<uint32_t> presentKeys;
        for (size_t k = 0; k < params.keys; ++k) {
            if (w.present[k]) { presentKeys.push_back(static_cast<uint32_t>(k)); }
        }
        for (size_t i = 0; i < params.reads; ++i) {
            probe.push_back(presentKeys[prng.nextIndex(presentKeys.size())]);
        }
    }

    // Churned probe: a shuffled stream over the churned SET, so consecutive reads hit different keys
    // (no single-key L1 residency) — the cache-realistic counterpart to hammering one hot key.
    std::vector<uint32_t> churnedProbe;
    {
        SplitMix64 prng(params.seed ^ 0xC0FFEE);
        std::vector<uint32_t> churnedKeys;
        for (size_t k = 1; k < w.churnedCount; ++k) {
            if (w.present[k]) { churnedKeys.push_back(static_cast<uint32_t>(k)); }
        }
        const size_t churnedReadCount = std::min<size_t>(params.reads, 1000000);
        if (!churnedKeys.empty()) {
            for (size_t i = 0; i < churnedReadCount; ++i) {
                churnedProbe.push_back(churnedKeys[prng.nextIndex(churnedKeys.size())]);
            }
        }
    }

    auto timeReads = [&](const char* what, auto&& fn) -> ReadResult {
        ReadResult best{1e18, 0};
        for (int pass = 0; pass < 3; ++pass) {
            uint64_t sum = 0;
            const double s = nowNs();
            for (size_t i = 0; i < probe.size(); ++i) {
                sum += fn(probe[i]);
            }
            const double e = nowNs();
            const double per = (e - s) / static_cast<double>(probe.size());
            if (per < best.nsPerOp) { best.nsPerOp = per; best.checksum = sum; }
        }
        (void)what;
        return best;
    };

    // A real string query must hash the probe key for the hash-based structures; ART hashes nothing.
    // Hashing INSIDE the timed loop is the fair comparison — it is precisely the cost ART avoids.
    const ReadResult rMvcc = timeReads("mvcc", [&](uint32_t k) {
        uint64_t v = 0; const uint64_t h = fnv1a(w.keyView[k]); mvcc.lookupHead(w.keyView[k], h, v); return v;
    });
    const ReadResult rHamt = timeReads("hamt", [&](uint32_t k) {
        uint64_t v = 0; const uint64_t h = fnv1a(w.keyView[k]); hamt.lookup(hroot, w.keyView[k], h, v); return v;
    });
    const ReadResult rArt = timeReads("art", [&](uint32_t k) {
        uint64_t v = 0; art.lookup(aroot, w.keyView[k], v); return v;
    });

    // --- single hot key, as-of at version T (L1-resident: isolates COMPUTE, shown only as a contrast) ---
    // Reading one key repeatedly keeps its whole path in L1, so this measures compute (hashing), not
    // memory — which is why ART (no hash) looks fastest here. The cache-realistic churned-set numbers
    // are in the snapshot-lag sweep below; do not compare these three across structures.
    auto timeHot = [&](auto&& fn) -> double {
        double best = 1e18;
        const size_t iters = params.asOfReads;
        for (int pass = 0; pass < 3; ++pass) {
            uint64_t sum = 0;
            const double s = nowNs();
            for (size_t i = 0; i < iters; ++i) { sum += fn(); }
            const double e = nowNs();
            const double per = (e - s) / static_cast<double>(iters);
            if (per < best) { best = per; }
            asm volatile("" : : "r"(sum) : "memory");
        }
        return best;
    };
    const double skMvcc = timeHot([&]() {
        const uint64_t h = fnv1a(w.keyView[w.hotKey]);
        uint64_t v = 0; mvcc.lookupAsOf(w.keyView[w.hotKey], h, static_cast<uint32_t>(T), v); return v;
    });
    const double skHamt = timeHot([&]() {
        const uint64_t h = fnv1a(w.keyView[w.hotKey]);
        uint64_t v = 0; hamt.lookup(hamtRoots[T], w.keyView[w.hotKey], h, v); return v;
    });
    const double skArt = timeHot([&]() {
        uint64_t v = 0; art.lookup(artRoots[T], w.keyView[w.hotKey], v); return v;
    });

    // --- memory ---
    const size_t mvccBytes = mvcc.retainedBytes();
    const size_t hamtBytes = Hamt::retainedBytes(hamtRoots);
    const size_t artBytes = Art::retainedBytes(artRoots);

    const double toMiB = 1.0 / (1024.0 * 1024.0);
    const double perCommit = static_cast<double>(params.parts);

    std::printf("  %-26s | %12s | %12s | %12s\n", "metric", "mvcc-hash", "HAMT", "ART");
    std::printf("  ---------------------------+--------------+--------------+--------------\n");
    std::printf("  %-26s | %9.1f ns | %9.1f ns | %9.1f ns\n", "HEAD read (point lookup)",
                rMvcc.nsPerOp, rHamt.nsPerOp, rArt.nsPerOp);
    std::printf("  %-26s | %8.2f us | %8.2f us | %8.2f us\n", "submit (one commit)",
                mvccBuildNs / perCommit / 1000.0, hamtBuildNs / perCommit / 1000.0, artBuildNs / perCommit / 1000.0);
    std::printf("  %-26s | %9.2f ns | %9.2f ns | %9.2f ns\n", "  per write",
                mvccBuildNs / static_cast<double>(params.parts * W),
                hamtBuildNs / static_cast<double>(params.parts * W),
                artBuildNs / static_cast<double>(params.parts * W));
    std::printf("  %-26s | %8.2f MiB | %8.2f MiB | %8.2f MiB\n", "retained memory",
                mvccBytes * toMiB, hamtBytes * toMiB, artBytes * toMiB);
    std::printf("\n  (read checksums: mvcc=%llu hamt=%llu art=%llu — must match)\n",
                (unsigned long long)rMvcc.checksum, (unsigned long long)rHamt.checksum,
                (unsigned long long)rArt.checksum);
    std::printf("  single-key contrast (one hot key as-of v%zu, L1-resident, COMPUTE-only — not comparable):"
                " mvcc=%.0fns hamt=%.0fns art=%.0fns\n", T, skMvcc, skHamt, skArt);

    // --- snapshot-lag sweep: TuringDB's normal case --------------------------------------------
    // A session opens a snapshot at some commit, then concurrent submits advance HEAD. By the time
    // it reads, the snapshot is `lag` commits behind. For the multiversion hash that read is an
    // as-of read (filter past every newer version of the key); for the tries it is reading an older
    // retained root — identical cost to HEAD. Both columns stream a shuffle over many distinct keys
    // (cache-realistic): a high-cardinality set (rarely rewritten → short chains) and the churned set
    // (frequently rewritten → deep-ish chains). The signal is how each structure's read scales with lag.
    const size_t sweepReads = std::min<size_t>(probe.size(), 200000);
    auto timeAsOfUniform = [&](size_t version, auto&& fn) -> double {
        double best = 1e18;
        for (int pass = 0; pass < 2; ++pass) {
            uint64_t sum = 0;
            const double s = nowNs();
            for (size_t i = 0; i < sweepReads; ++i) { sum += fn(probe[i], version); }
            const double e = nowNs();
            const double per = (e - s) / static_cast<double>(sweepReads);
            if (per < best) { best = per; }
            asm volatile("" : : "r"(sum) : "memory");
        }
        return best;
    };
    // Churned SET, streamed (shuffled over many churned keys) — cache-realistic, deep-ish chains.
    const size_t sweepChurnedReads = std::min<size_t>(churnedProbe.size(), 100000);
    auto timeChurnedStream = [&](size_t version, auto&& fn) -> double {
        if (sweepChurnedReads == 0) {
            return 0.0;
        }
        double best = 1e18;
        for (int pass = 0; pass < 2; ++pass) {
            uint64_t sum = 0;
            const double s = nowNs();
            for (size_t i = 0; i < sweepChurnedReads; ++i) { sum += fn(churnedProbe[i], version); }
            const double e = nowNs();
            const double per = (e - s) / static_cast<double>(sweepChurnedReads);
            if (per < best) { best = per; }
            asm volatile("" : : "r"(sum) : "memory");
        }
        return best;
    };

    std::printf("\n  Read latency vs snapshot lag (commits behind HEAD) — the reader is on a PAST snapshot\n");
    std::printf("  both columns stream a shuffle of distinct keys (cache-realistic); churned set avg chain depth %.0f\n",
                avgChurnedDepth);
    std::printf("  %-7s | %-31s | %-31s\n", "", "high-cardinality key (rare writes)", "churned key set (frequent writes)");
    std::printf("  %-7s | %9s %9s %9s | %9s %9s %9s\n", "lag", "mvcc", "hamt", "art", "mvcc", "hamt", "art");
    std::printf("  --------+---------------------------------+--------------------------------\n");

    std::vector<size_t> lags {0, 2, 10, 100, 1000, params.parts - 1};
    for (size_t lag : lags) {
        if (lag >= params.parts) {
            continue;
        }
        const size_t version = (params.parts - 1) - lag;

        const double uMvcc = timeAsOfUniform(version, [&](uint32_t k, size_t ver) {
            const uint64_t h = fnv1a(w.keyView[k]);
            uint64_t v = 0; mvcc.lookupAsOf(w.keyView[k], h, static_cast<uint32_t>(ver), v); return v;
        });
        const double uHamt = timeAsOfUniform(version, [&](uint32_t k, size_t ver) {
            const uint64_t h = fnv1a(w.keyView[k]);
            uint64_t v = 0; hamt.lookup(hamtRoots[ver], w.keyView[k], h, v); return v;
        });
        const double uArt = timeAsOfUniform(version, [&](uint32_t k, size_t ver) {
            uint64_t v = 0; art.lookup(artRoots[ver], w.keyView[k], v); return v;
        });

        const double cMvcc = timeChurnedStream(version, [&](uint32_t k, size_t ver) {
            const uint64_t h = fnv1a(w.keyView[k]);
            uint64_t v = 0; mvcc.lookupAsOf(w.keyView[k], h, static_cast<uint32_t>(ver), v); return v;
        });
        const double cHamt = timeChurnedStream(version, [&](uint32_t k, size_t ver) {
            const uint64_t h = fnv1a(w.keyView[k]);
            uint64_t v = 0; hamt.lookup(hamtRoots[ver], w.keyView[k], h, v); return v;
        });
        const double cArt = timeChurnedStream(version, [&](uint32_t k, size_t ver) {
            uint64_t v = 0; art.lookup(artRoots[ver], w.keyView[k], v); return v;
        });

        std::printf("  %7zu | %7.0fns %7.0fns %7.0fns | %7.0fns %7.0fns %7.0fns\n",
                    lag, uMvcc, uHamt, uArt, cMvcc, cHamt, cArt);
    }
    std::printf("  (lag 0 = reading true HEAD. The tries are flat in lag; the multiversion hash grows with how many\n");
    std::printf("   times the key was rewritten in the lag window — negligible for a high-cardinality key, but it\n");
    std::printf("   climbs for the churned set as the snapshot falls behind. Reads stay lock-free under full retention.)\n");
}

bool parseSize(int argc, char** argv, int& i, const char* flag, size_t& out) {
    if (std::string_view(argv[i]) == flag && i + 1 < argc) {
        out = static_cast<size_t>(std::strtoull(argv[++i], nullptr, 10));
        return true;
    }
    return false;
}

} // namespace

int main(int argc, char** argv) {
    BenchParams params;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        size_t v = 0;
        if (arg == "--help" || arg == "-h") {
            std::printf("index_bench [--keys N --writes W --parts P --key-len L --reads R --branch B\n"
                        "             --churned N --churned-writes W]\n"
                        "  --churned N        size of the low-cardinality churned key set, streamed in the\n"
                        "                     snapshot-lag sweep (default 1024; fewer => deeper version chains)\n"
                        "  --churned-writes W churned writes per commit (default ~half; more => deeper chains)\n");
            return EXIT_SUCCESS;
        } else if (parseSize(argc, argv, i, "--keys", v)) {
            params.keys = v;
        } else if (parseSize(argc, argv, i, "--writes", v)) {
            params.writesPerCommit = v;
        } else if (parseSize(argc, argv, i, "--parts", v)) {
            params.parts = v;
        } else if (parseSize(argc, argv, i, "--key-len", v)) {
            params.keyLenBytes = v;
        } else if (parseSize(argc, argv, i, "--reads", v)) {
            params.reads = v;
        } else if (parseSize(argc, argv, i, "--branch", v)) {
            size_t bits = 1;
            while ((size_t(1) << bits) < v) { ++bits; }
            params.hamtBits = bits;
        } else if (parseSize(argc, argv, i, "--churned", v)) {
            params.churnedKeys = v;
        } else if (parseSize(argc, argv, i, "--churned-writes", v)) {
            params.churnedWrites = v;
        } else {
            std::printf("unknown argument: %s (try --help)\n", argv[i]);
            return EXIT_FAILURE;
        }
    }
    if (params.keys == 0 || params.parts == 0 || params.writesPerCommit == 0 || params.keyLenBytes == 0) {
        std::printf("keys, parts, writes, key-len must be > 0\n");
        return EXIT_FAILURE;
    }
    run(params);
    return EXIT_SUCCESS;
}
