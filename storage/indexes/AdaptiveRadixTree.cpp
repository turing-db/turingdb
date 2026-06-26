#include "AdaptiveRadixTree.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <new>
#include <algorithm>

#if defined(__x86_64__)
#include <immintrin.h>
#define ART_HAS_X86_SIMD 1
#else
#define ART_HAS_X86_SIMD 0
#endif

#include "BioAssert.h"
#include "BumpPtrAllocator.h"

using namespace db;

// All node structures and the read/write machinery live in this anonymous namespace; the header keeps
// the root as an opaque void* so none of this leaks out. See docs/ART.md for the design.
namespace {

// Folded into the parent's inline prefix[] by path compression; a longer shared run is carried by a
// chain of prefix nodes (§3.3).
constexpr size_t ART_MAX_PREFIX = 16;

// Nodes and leaves are bump-allocated from a per-tree arena (this slab size) so that a tree's memory is
// co-located rather than scattered across individual heap allocations -- this cuts the cache/TLB misses
// that dominate the descent once the index spills the last-level cache. The arena never frees individual
// objects, so node growth (N4->N16->...) leaves the outgrown node as dead arena space; that is bounded
// (a node grows at most three times) and reclaimed wholesale when the tree is destroyed.
constexpr size_t ART_ARENA_SLAB_SIZE = 1 << 20;

// Reserved end-of-key byte. Appending it logically to every key keeps the key set prefix-free so that
// e.g. "foo" and "foobar" can coexist; real keys must therefore not contain a 0x00 byte.
constexpr uint8_t ART_TERMINATOR = 0;

// Leaf-edge tagging (§3.4). A child slot (or the root) that points to a leaf carries, in the otherwise
// unused bits of the pointer, a 16-bit fingerprint of the leaf's full key and a leaf marker. This
// assumes 48-bit user-space pointers (top 16 bits free) and >=2-aligned leaves (bit 0 free), which
// holds on x86-64 and aarch64 Linux.
constexpr uintptr_t ART_LEAF_BIT = 1;
constexpr uintptr_t ART_POINTER_MASK = 0x0000FFFFFFFFFFFEull;  // keep bits 1..47 (clear marker + fingerprint)

enum class ARTNodeType : uint8_t {
    Node4,
    Node16,
    Node48,
    Node256,
};

// Shared header of every inner node: the discriminating byte -> child mapping differs per type below.
struct ARTNode {
    ARTNodeType type {ARTNodeType::Node4};
    uint16_t numChildren {0};
    uint8_t prefixLength {0};
    uint8_t prefix[ART_MAX_PREFIX];
};

struct ARTNode4 : ARTNode {
    uint8_t keys[4];
    ARTNode* children[4];
};

struct ARTNode16 : ARTNode {
    uint8_t keys[16];
    ARTNode* children[16];
};

struct ARTNode48 : ARTNode {
    uint8_t childIndex[256];   // byte -> slot + 1 (0 means absent)
    ARTNode* children[48];
};

struct ARTNode256 : ARTNode {
    ARTNode* children[256];
};

// A leaf is a single heap block laid out as [ARTLeaf header][key bytes][padding][value bytes]. The
// full key is stored so the lookup can verify it (§4.3); the value is an opaque blob of valueSize bytes.
struct ARTLeaf {
    uint32_t keyLength {0};

    const char* keyData() const { return reinterpret_cast<const char*>(this) + sizeof(ARTLeaf); }
    std::string_view key() const { return std::string_view(keyData(), keyLength); }
};

size_t alignUp(size_t value, size_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

// The query key seen one byte past its end yields the terminator, so descent and insertion treat every
// key as if it ended with ART_TERMINATOR.
uint8_t logicalByteAt(std::string_view key, size_t index) {
    return index < key.size() ? static_cast<uint8_t>(key[index]) : ART_TERMINATOR;
}

// First logical byte position (from `from`) at which two distinct keys differ. The caller guarantees
// the keys are not equal, so the loop always terminates.
size_t firstDifference(std::string_view a, std::string_view b, size_t from) {
    size_t index = from;
    while (logicalByteAt(a, index) == logicalByteAt(b, index)) {
        ++index;
    }
    return index;
}

// Cheap, position-independent fingerprint of the whole key (§3.4): hardware CRC32 where available, a
// software hash otherwise. Both sides of a comparison run the same function within a build, so the
// fingerprint stays consistent.
#if defined(__SSE4_2__)
uint16_t fingerprintOfKey(std::string_view key) {
    const char* data = key.data();
    const size_t length = key.size();
    uint64_t crc = 0;
    size_t index = 0;

    for (; index + 8 <= length; index += 8) {
        uint64_t word;
        memcpy(&word, data + index, 8);
        crc = _mm_crc32_u64(crc, word);
    }

    if (index < length) {
        uint64_t word = 0;
        memcpy(&word, data + index, length - index);
        crc = _mm_crc32_u64(crc, word);
    }

    return static_cast<uint16_t>(crc ^ (crc >> 13));
}
#else
uint16_t fingerprintOfKey(std::string_view key) {
    uint64_t hash = 1469598103934665603ull;   // FNV-1a 64-bit
    for (const char byte : key) {
        hash ^= static_cast<uint8_t>(byte);
        hash *= 1099511628211ull;
    }
    return static_cast<uint16_t>(hash ^ (hash >> 13));
}
#endif

ARTNode* tagLeaf(ARTLeaf* leaf, uint16_t fingerprint) {
    const uintptr_t bits = reinterpret_cast<uintptr_t>(leaf)
                           | (static_cast<uintptr_t>(fingerprint) << 48)
                           | ART_LEAF_BIT;
    return reinterpret_cast<ARTNode*>(bits);
}

bool isLeaf(const ARTNode* node) {
    return (reinterpret_cast<uintptr_t>(node) & ART_LEAF_BIT) != 0;
}

uint16_t leafFingerprint(const ARTNode* node) {
    return static_cast<uint16_t>(reinterpret_cast<uintptr_t>(node) >> 48);
}

ARTLeaf* leafOf(const ARTNode* node) {
    return reinterpret_cast<ARTLeaf*>(reinterpret_cast<uintptr_t>(node) & ART_POINTER_MASK);
}

ARTNode4* newNode4(BumpPtrAllocator& arena) {
    ARTNode4* node = arena.create<ARTNode4>();
    node->type = ARTNodeType::Node4;
    return node;
}

ARTNode16* newNode16(BumpPtrAllocator& arena) {
    ARTNode16* node = arena.create<ARTNode16>();
    node->type = ARTNodeType::Node16;
    return node;
}

ARTNode48* newNode48(BumpPtrAllocator& arena) {
    ARTNode48* node = arena.create<ARTNode48>();
    node->type = ARTNodeType::Node48;
    memset(node->childIndex, 0, sizeof(node->childIndex));
    return node;
}

ARTNode256* newNode256(BumpPtrAllocator& arena) {
    ARTNode256* node = arena.create<ARTNode256>();
    node->type = ARTNodeType::Node256;
    memset(node->children, 0, sizeof(node->children));
    return node;
}

void copyHeader(ARTNode* destination, const ARTNode* source) {
    destination->numChildren = source->numChildren;
    destination->prefixLength = source->prefixLength;
    memcpy(destination->prefix, source->prefix, ART_MAX_PREFIX);
}

// Returns the slot holding the child for `byte`, or nullptr if absent. Node4/Node16 are searched with
// one SIMD compare (§4.2); Node48/Node256 are directly indexed.
ARTNode** findChild(ARTNode* node, uint8_t byte) {
    switch (node->type) {
        case ARTNodeType::Node4: {
            ARTNode4* typed = static_cast<ARTNode4*>(node);
#if ART_HAS_X86_SIMD
            uint32_t packed;
            memcpy(&packed, typed->keys, sizeof(packed));
            const __m128i needle = _mm_set1_epi8(static_cast<char>(byte));
            const __m128i stored = _mm_cvtsi32_si128(static_cast<int>(packed));
            unsigned match = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(needle, stored)));
            match &= (1u << typed->numChildren) - 1u;
            if (match == 0) {
                return nullptr;
            }
            return &typed->children[__builtin_ctz(match)];
#else
            for (size_t i = 0; i < typed->numChildren; ++i) {
                if (typed->keys[i] == byte) {
                    return &typed->children[i];
                }
            }
            return nullptr;
#endif
        }
        break;
        case ARTNodeType::Node16: {
            ARTNode16* typed = static_cast<ARTNode16*>(node);
#if ART_HAS_X86_SIMD
            const __m128i needle = _mm_set1_epi8(static_cast<char>(byte));
            const __m128i stored = _mm_loadu_si128(reinterpret_cast<const __m128i*>(typed->keys));
            unsigned match = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(needle, stored)));
            match &= (typed->numChildren >= 16) ? 0xFFFFu : ((1u << typed->numChildren) - 1u);
            if (match == 0) {
                return nullptr;
            }
            return &typed->children[__builtin_ctz(match)];
#else
            for (size_t i = 0; i < typed->numChildren; ++i) {
                if (typed->keys[i] == byte) {
                    return &typed->children[i];
                }
            }
            return nullptr;
#endif
        }
        break;
        case ARTNodeType::Node48: {
            ARTNode48* typed = static_cast<ARTNode48*>(node);
            const uint8_t slot = typed->childIndex[byte];
            if (slot == 0) {
                return nullptr;
            }
            return &typed->children[slot - 1];
        }
        break;
        case ARTNodeType::Node256: {
            ARTNode256* typed = static_cast<ARTNode256*>(node);
            if (typed->children[byte] == nullptr) {
                return nullptr;
            }
            return &typed->children[byte];
        }
        break;
    }
    return nullptr;
}

// Adds `byte -> child` to `node`, growing it to the next node type when full. Returns the node to
// install in the parent slot: the same node if it had room, or the grown replacement (the outgrown node
// is left as dead arena space, reclaimed when the tree is destroyed). `child` and the existing children
// are never touched.
ARTNode* addChild(BumpPtrAllocator& arena, ARTNode* node, uint8_t byte, ARTNode* child) {
    switch (node->type) {
        case ARTNodeType::Node4: {
            ARTNode4* typed = static_cast<ARTNode4*>(node);
            if (typed->numChildren < 4) {
                typed->keys[typed->numChildren] = byte;
                typed->children[typed->numChildren] = child;
                ++typed->numChildren;
                return typed;
            }
            ARTNode16* grown = newNode16(arena);
            copyHeader(grown, typed);
            for (size_t i = 0; i < 4; ++i) {
                grown->keys[i] = typed->keys[i];
                grown->children[i] = typed->children[i];
            }
            grown->keys[4] = byte;
            grown->children[4] = child;
            grown->numChildren = 5;
            return grown;
        }
        break;
        case ARTNodeType::Node16: {
            ARTNode16* typed = static_cast<ARTNode16*>(node);
            if (typed->numChildren < 16) {
                typed->keys[typed->numChildren] = byte;
                typed->children[typed->numChildren] = child;
                ++typed->numChildren;
                return typed;
            }
            ARTNode48* grown = newNode48(arena);
            copyHeader(grown, typed);
            for (size_t i = 0; i < 16; ++i) {
                grown->children[i] = typed->children[i];
                grown->childIndex[typed->keys[i]] = static_cast<uint8_t>(i + 1);
            }
            grown->children[16] = child;
            grown->childIndex[byte] = 17;
            grown->numChildren = 17;
            return grown;
        }
        break;
        case ARTNodeType::Node48: {
            ARTNode48* typed = static_cast<ARTNode48*>(node);
            if (typed->numChildren < 48) {
                const uint16_t slot = typed->numChildren;
                typed->children[slot] = child;
                typed->childIndex[byte] = static_cast<uint8_t>(slot + 1);
                ++typed->numChildren;
                return typed;
            }
            ARTNode256* grown = newNode256(arena);
            copyHeader(grown, typed);
            for (size_t b = 0; b < 256; ++b) {
                const uint8_t slot = typed->childIndex[b];
                if (slot != 0) {
                    grown->children[b] = typed->children[slot - 1];
                }
            }
            grown->children[byte] = child;
            grown->numChildren = 49;
            return grown;
        }
        break;
        case ARTNodeType::Node256: {
            ARTNode256* typed = static_cast<ARTNode256*>(node);
            typed->children[byte] = child;
            ++typed->numChildren;
            return typed;
        }
        break;
    }
    return node;
}

ARTLeaf* allocLeaf(BumpPtrAllocator& arena,
                   std::string_view key,
                   const void* value,
                   size_t valueSize,
                   size_t valueAlignment) {
    const size_t valueOffset = alignUp(sizeof(ARTLeaf) + key.size(), valueAlignment);
    const size_t totalSize = valueOffset + valueSize;
    const size_t blockAlignment = std::max(alignof(ARTLeaf), valueAlignment);

    void* memory = arena.allocate(totalSize, blockAlignment);
    ARTLeaf* leaf = new (memory) ARTLeaf();
    leaf->keyLength = static_cast<uint32_t>(key.size());

    char* bytes = static_cast<char*>(memory);
    memcpy(bytes + sizeof(ARTLeaf), key.data(), key.size());
    memcpy(bytes + valueOffset, value, valueSize);
    return leaf;
}

// The tree engine: the arena that owns every node/leaf plus the opaque value layout. Cheap to construct
// (a pointer and two size_t), so AdaptiveRadixTreeBase makes one per call rather than storing it.
struct ARTOps {
    BumpPtrAllocator* arena {nullptr};
    size_t valueSize {0};
    size_t valueAlignment {0};

    void* valueAddress(ARTLeaf* leaf) const;
    ARTNode* makeLeaf(std::string_view key, const void* value) const;
    ARTNode* splitLeaves(ARTLeaf* leaf,
                         std::string_view key,
                         const void* value,
                         size_t depth,
                         size_t difference) const;
    ARTNode* insert(ARTNode* slot,
                    std::string_view key,
                    const void* value,
                    size_t depth,
                    bool& inserted) const;
    ARTLeaf* findLeaf(ARTNode* root, std::string_view key) const;
    bool find(ARTNode* root, std::string_view key, void* value) const;
    bool contains(ARTNode* root, std::string_view key) const;
    void findBatch(ARTNode* root,
                   const std::string_view* keys,
                   size_t count,
                   uint8_t* found,
                   void* values) const;
};

void* ARTOps::valueAddress(ARTLeaf* leaf) const {
    char* bytes = reinterpret_cast<char*>(leaf);
    return bytes + alignUp(sizeof(ARTLeaf) + leaf->keyLength, valueAlignment);
}

ARTNode* ARTOps::makeLeaf(std::string_view key, const void* value) const {
    ARTLeaf* leaf = allocLeaf(*arena, key, value, valueSize, valueAlignment);
    return tagLeaf(leaf, fingerprintOfKey(key));
}

// Replaces a single leaf with a Node4 (chain) that branches the existing leaf and the new key at their
// first differing byte. Their shared run [depth, difference) becomes path-compressed prefix; a run
// longer than ART_MAX_PREFIX is carried by a chain of single-child prefix nodes (§3.3). When one key is
// a prefix of the other, its diverging byte is the terminator, so it lands on its own child slot.
ARTNode* ARTOps::splitLeaves(ARTLeaf* leaf,
                             std::string_view key,
                             const void* value,
                             size_t depth,
                             size_t difference) const {
    ARTNode4* head = newNode4(*arena);
    ARTNode* current = head;
    size_t start = depth;

    while (difference - start > ART_MAX_PREFIX) {
        current->prefixLength = static_cast<uint8_t>(ART_MAX_PREFIX);
        memcpy(current->prefix, key.data() + start, ART_MAX_PREFIX);

        ARTNode4* next = newNode4(*arena);
        addChild(*arena, current, logicalByteAt(key, start + ART_MAX_PREFIX), next);
        current = next;
        start += ART_MAX_PREFIX + 1;
    }

    current->prefixLength = static_cast<uint8_t>(difference - start);
    memcpy(current->prefix, key.data() + start, difference - start);

    ARTNode* existingEdge = tagLeaf(leaf, fingerprintOfKey(leaf->key()));
    current = addChild(*arena, current, logicalByteAt(leaf->key(), difference), existingEdge);
    current = addChild(*arena, current, logicalByteAt(key, difference), makeLeaf(key, value));
    return head;
}

// Inserts (or overwrites) `key` under `slot`, returning the slot value the parent must store (the same
// pointer, or a new node when a leaf split or a node grow replaced it). `inserted` is set to false only
// when an existing key's value was overwritten.
ARTNode* ARTOps::insert(ARTNode* slot,
                        std::string_view key,
                        const void* value,
                        size_t depth,
                        bool& inserted) const {
    if (slot == nullptr) {
        inserted = true;
        return makeLeaf(key, value);
    } else if (isLeaf(slot)) {
        ARTLeaf* leaf = leafOf(slot);
        if (leaf->key() == key) {
            memcpy(valueAddress(leaf), value, valueSize);   // same key: fingerprint and edge unchanged
            inserted = false;
            return slot;
        }
        const size_t difference = firstDifference(leaf->key(), key, depth);
        inserted = true;
        return splitLeaves(leaf, key, value, depth, difference);
    }

    ARTNode* node = slot;

    if (node->prefixLength > 0) {
        size_t shared = 0;
        while (shared < node->prefixLength
               && logicalByteAt(key, depth + shared) == node->prefix[shared]) {
            ++shared;
        }

        if (shared != node->prefixLength) {
            // The key leaves the compressed prefix early: branch the existing subtree and the new leaf
            // at `shared`, keeping the unmatched tail of the prefix on the existing node.
            ARTNode4* branch = newNode4(*arena);
            branch->prefixLength = static_cast<uint8_t>(shared);
            memcpy(branch->prefix, node->prefix, shared);

            const uint8_t existingByte = node->prefix[shared];
            node->prefixLength = static_cast<uint8_t>(node->prefixLength - shared - 1);
            memmove(node->prefix, node->prefix + shared + 1, node->prefixLength);

            ARTNode* parent = addChild(*arena, branch, existingByte, node);
            parent = addChild(*arena, parent, logicalByteAt(key, depth + shared), makeLeaf(key, value));
            inserted = true;
            return parent;
        }

        depth += node->prefixLength;
    }

    const uint8_t branchByte = logicalByteAt(key, depth);
    ARTNode** childSlot = findChild(node, branchByte);
    if (childSlot != nullptr) {
        *childSlot = insert(*childSlot, key, value, depth + 1, inserted);
        return node;
    }

    inserted = true;
    return addChild(*arena, node, branchByte, makeLeaf(key, value));
}

// Serial descent (§4.1): match compressed prefixes, find the child by its discriminating byte, reject
// a leaf edge whose fingerprint differs without loading it (§4.5), and verify the full key at the leaf.
ARTLeaf* ARTOps::findLeaf(ARTNode* root, std::string_view key) const {
    const uint16_t queryFingerprint = fingerprintOfKey(key);
    ARTNode* current = root;
    size_t depth = 0;

    while (current != nullptr) {
        if (isLeaf(current)) {
            ARTLeaf* leaf = leafOf(current);   // only reached when the whole tree is a single leaf
            return leaf->key() == key ? leaf : nullptr;
        }

        if (current->prefixLength > 0) {
            const bool keyTooShort = depth + current->prefixLength > key.size();
            if (keyTooShort) {
                return nullptr;
            }

            const bool prefixMismatch = memcmp(current->prefix, key.data() + depth, current->prefixLength) != 0;
            if (prefixMismatch) {
                return nullptr;
            }

            depth += current->prefixLength;
        }

        ARTNode** slot = findChild(current, logicalByteAt(key, depth));
        if (slot == nullptr) {
            return nullptr;
        }

        ARTNode* next = *slot;
        if (isLeaf(next)) {
            if (leafFingerprint(next) != queryFingerprint) {
                return nullptr;                            // miss-reject: no leaf load
            }
            ARTLeaf* leaf = leafOf(next);
            return leaf->key() == key ? leaf : nullptr;    // full-key verify
        }

        current = next;
        ++depth;
    }

    return nullptr;
}

bool ARTOps::find(ARTNode* root, std::string_view key, void* value) const {
    ARTLeaf* leaf = findLeaf(root, key);
    if (leaf == nullptr) {
        return false;
    }
    memcpy(value, valueAddress(leaf), valueSize);
    return true;
}

bool ARTOps::contains(ARTNode* root, std::string_view key) const {
    return findLeaf(root, key) != nullptr;
}

// AMAC 8-way batched descent (§4.4): the same logic as findLeaf, run over up to 8 keys at once with one
// level of progress per inner loop pass so each lane's dependent load is overlapped with the others'
// work. A matched leaf edge is prefetched and verified on the next pass to keep its load overlapped.
void ARTOps::findBatch(ARTNode* root,
                       const std::string_view* keys,
                       size_t count,
                       uint8_t* found,
                       void* values) const {
    constexpr int Width = 8;

    ARTNode* node[Width];
    size_t depth[Width];
    const char* keyData[Width];
    size_t keyLength[Width];
    uint16_t queryFingerprint[Width];
    size_t resultIndex[Width];
    bool done[Width];

    char* valueBytes = static_cast<char*>(values);

    for (size_t base = 0; base < count; base += Width) {
        const int lanes = static_cast<int>(std::min<size_t>(Width, count - base));
        int remaining = lanes;

        for (int j = 0; j < lanes; ++j) {
            const std::string_view key = keys[base + j];
            node[j] = root;
            depth[j] = 0;
            keyData[j] = key.data();
            keyLength[j] = key.size();
            queryFingerprint[j] = fingerprintOfKey(key);
            resultIndex[j] = base + j;
            done[j] = false;
            found[base + j] = 0;
            __builtin_prefetch(root);
        }

        while (remaining > 0) {
            for (int j = 0; j < lanes; ++j) {
                if (done[j]) {
                    continue;
                }

                ARTNode* current = node[j];
                if (current == nullptr) {
                    done[j] = true;
                    --remaining;
                    continue;
                }

                if (isLeaf(current)) {
                    ARTLeaf* leaf = leafOf(current);
                    if (leaf->key() == std::string_view(keyData[j], keyLength[j])) {
                        memcpy(valueBytes + resultIndex[j] * valueSize, valueAddress(leaf), valueSize);
                        found[resultIndex[j]] = 1;
                    }
                    done[j] = true;
                    --remaining;
                    continue;
                }

                if (current->prefixLength > 0) {
                    const bool keyTooShort = depth[j] + current->prefixLength > keyLength[j];
                    const bool prefixMismatch = !keyTooShort
                        && memcmp(current->prefix, keyData[j] + depth[j], current->prefixLength) != 0;
                    if (keyTooShort || prefixMismatch) {
                        done[j] = true;
                        --remaining;
                        continue;
                    }
                    depth[j] += current->prefixLength;
                }

                const uint8_t branchByte = depth[j] < keyLength[j]
                                           ? static_cast<uint8_t>(keyData[j][depth[j]])
                                           : ART_TERMINATOR;
                ARTNode** slot = findChild(current, branchByte);
                if (slot == nullptr) {
                    done[j] = true;
                    --remaining;
                    continue;
                }

                ARTNode* next = *slot;
                if (isLeaf(next)) {
                    if (leafFingerprint(next) != queryFingerprint[j]) {
                        done[j] = true;
                        --remaining;
                        continue;
                    }
                    __builtin_prefetch(leafOf(next));
                    node[j] = next;          // verify the now-prefetched leaf on the next pass
                    continue;
                }

                __builtin_prefetch(next);
                node[j] = next;
                ++depth[j];
            }
        }
    }
}

}

AdaptiveRadixTreeBase::AdaptiveRadixTreeBase(size_t valueSize, size_t valueAlignment)
    : _arena(new BumpPtrAllocator(ART_ARENA_SLAB_SIZE)),
      _valueSize(valueSize),
      _valueAlignment(valueAlignment)
{
}

AdaptiveRadixTreeBase::~AdaptiveRadixTreeBase() {
    delete static_cast<BumpPtrAllocator*>(_arena);   // frees every node and leaf at once
}

bool AdaptiveRadixTreeBase::insert(std::string_view key, const void* value) {
    bioassert(key.find('\0') == std::string_view::npos,
              "AdaptiveRadixTree keys must not contain a 0x00 byte (reserved as the end-of-key marker)");

    const ARTOps ops {static_cast<BumpPtrAllocator*>(_arena), _valueSize, _valueAlignment};
    bool inserted = false;
    _root = ops.insert(static_cast<ARTNode*>(_root), key, value, 0, inserted);
    if (inserted) {
        ++_size;
    }
    return inserted;
}

bool AdaptiveRadixTreeBase::find(std::string_view key, void* value) const {
    const ARTOps ops {static_cast<BumpPtrAllocator*>(_arena), _valueSize, _valueAlignment};
    return ops.find(static_cast<ARTNode*>(_root), key, value);
}

bool AdaptiveRadixTreeBase::contains(std::string_view key) const {
    const ARTOps ops {static_cast<BumpPtrAllocator*>(_arena), _valueSize, _valueAlignment};
    return ops.contains(static_cast<ARTNode*>(_root), key);
}

void AdaptiveRadixTreeBase::findBatch(const std::string_view* keys,
                                      size_t count,
                                      uint8_t* found,
                                      void* values) const {
    const ARTOps ops {static_cast<BumpPtrAllocator*>(_arena), _valueSize, _valueAlignment};
    ops.findBatch(static_cast<ARTNode*>(_root), keys, count, found, values);
}
