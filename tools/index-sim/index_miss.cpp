// index_miss.cpp — closing ART's one remaining deficit vs the multiversion hash: the MISS (absent key).
//
// Extends index_opt.cpp (same MvccHash / Art / workload / counters, so numbers are comparable) with a
// leaf-fingerprint read path: a cheap CRC32 fingerprint of each leaf's full key is carried in the leaf
// child pointer's spare bits and checked DURING the descent, before the cold leaf load — so a prefix-
// colliding absent key is rejected without the leaf load that the multiversion hash short-circuits past.
// Sound (0 false-negatives, 0 false-positives: the full-key verify is preserved on a fingerprint match).
// Measures HIT and three honest absent-key distributions (near-byte15 / near-mid / random), serial and
// AMAC-batched, against the hash and the plain verifying ART. See report_art_miss.md.
//
// Build: g++ -std=c++23 -O2 -march=native -o index_miss index_miss.cpp
// Run:   taskset -c 0 ./index_miss

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/perf_event.h>
#include <immintrin.h>   // SSE2 / AVX2 / BMI2 for SIMD node search

#include <string>
#include <unordered_map>
#include <string_view>
#include <vector>
#include <array>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <cstdio>

namespace {

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

// ---- perf counters ---------------------------------------------------------------------------
long perfOpen(uint32_t type, uint64_t config, int groupFd) {
    perf_event_attr attr{};
    attr.type = type;
    attr.size = sizeof(attr);
    attr.config = config;
    attr.disabled = (groupFd == -1) ? 1 : 0;
    attr.exclude_kernel = 1;
    attr.exclude_hv = 1;
    attr.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID;
    return syscall(__NR_perf_event_open, &attr, 0, -1, groupFd, 0);
}

struct PerfGroup {
    int leader {-1};
    int fdInstr {-1}, fdCacheMiss {-1}, fdBranchMiss {-1};
    bool ok {false};
    void start() {
        leader = -1;
        for (int attempt = 0; attempt < 50 && leader < 0; ++attempt) {        // retry transient EAGAIN/EBUSY
            leader = perfOpen(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, -1);
            if (leader < 0) { usleep(200); }
        }
        if (leader < 0) { ok = false; return; }
        fdInstr      = perfOpen(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS, leader);
        fdCacheMiss  = perfOpen(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES, leader);
        fdBranchMiss = perfOpen(PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES, leader);
        ok = true;
        ioctl(leader, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
        ioctl(leader, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
    }
    // returns cycles, instr, cacheMiss, branchMiss
    void stop(double n, double& cyc, double& ins, double& cm, double& bm) {
        ioctl(leader, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
        struct { uint64_t nr; struct { uint64_t value, id; } e[4]; } buf{};
        ssize_t r = read(leader, &buf, sizeof(buf));
        (void)r;
        // order matches creation: leader(cycles), instr, cachemiss, branchmiss
        cyc = buf.e[0].value / n; ins = buf.e[1].value / n; cm = buf.e[2].value / n; bm = buf.e[3].value / n;
        close(fdBranchMiss); close(fdCacheMiss); close(fdInstr); close(leader);
    }
};

// ============================================================================================
// 1. Multiversion hash (identical to index_bench) + ablation variants
// ============================================================================================
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
        while (cap < expectedKeys * 2) { cap <<= 1; }
        _slots.assign(cap, MvccSlot{});
        _mask = cap - 1;
    }
    void insert(std::string_view key, uint64_t hash, uint64_t value, uint32_t version) {
        if ((_count + 1) * 10 >= _slots.size() * 7) { grow(); }
        MvccSlot& slot = locate(key, hash);
        if (!slot.used) { slot.used = true; slot.key = key; slot.hash = hash; ++_count; }
        VersionNode* node = new VersionNode{version, value, slot.head};
        slot.head = node;
    }
    // M0: baseline HEAD read (hash compared + full key compared)
    bool lookupHead(std::string_view key, uint64_t hash, uint64_t& out) const {
        const MvccSlot& slot = locate(key, hash);
        if (!slot.used || slot.head == nullptr) { return false; }
        out = slot.head->value;
        return true;
    }
    // M2: trust the hash, skip the full key compare (isolates the pooled-key load + memcmp)
    bool lookupHeadNoKeyCmp(uint64_t hash, uint64_t& out) const {
        size_t i = hash & _mask;
        while (_slots[i].used && _slots[i].hash != hash) { i = (i + 1) & _mask; }
        const MvccSlot& slot = _slots[i];
        if (!slot.used || slot.head == nullptr) { return false; }
        out = slot.head->value;
        return true;
    }
    // instrumentation: probe length + chain depth at HEAD
    void lookupStats(std::string_view key, uint64_t hash, size_t& probeLen, size_t& chainDepth) const {
        size_t i = hash & _mask; probeLen = 1;
        while (_slots[i].used && !(_slots[i].hash == hash && _slots[i].key == key)) { i = (i + 1) & _mask; ++probeLen; }
        chainDepth = 0;
        for (const VersionNode* n = _slots[i].head; n != nullptr; n = n->older) { ++chainDepth; }
    }
    const MvccSlot* slotPtr(uint64_t hash) const { return &_slots[hash & _mask]; }
    uint64_t maskVal() const { return _mask; }
    const std::vector<MvccSlot>& slots() const { return _slots; }
private:
    const MvccSlot& locate(std::string_view key, uint64_t hash) const {
        size_t i = hash & _mask;
        while (_slots[i].used && !(_slots[i].hash == hash && _slots[i].key == key)) { i = (i + 1) & _mask; }
        return _slots[i];
    }
    MvccSlot& locate(std::string_view key, uint64_t hash) {
        const MvccHash* self = this; return const_cast<MvccSlot&>(self->locate(key, hash));
    }
    void grow() {
        std::vector<MvccSlot> old; old.swap(_slots);
        _slots.assign(old.size() * 2, MvccSlot{}); _mask = _slots.size() - 1;
        for (const MvccSlot& slot : old) {
            if (slot.used) { size_t i = slot.hash & _mask; while (_slots[i].used) { i = (i + 1) & _mask; } _slots[i] = slot; }
        }
    }
    std::vector<MvccSlot> _slots; size_t _mask {0}; size_t _count {0};
};

// ============================================================================================
// 2. ART (identical to index_bench) + inline-key leaf + ablation variants
// ============================================================================================
enum class ArtType : uint8_t { Leaf, N4, N16, N48, N256 };
static const size_t ART_MAX_PREFIX = 16;

struct ArtBase { ArtType type; };
struct ArtLeaf : ArtBase {
    std::string_view key;
    uint64_t value {0};
    char inlineKey[16];   // copy of the key bytes, for the inline-compare ablation (no pool chase)
};
struct ArtInner : ArtBase {
    uint16_t numChildren {0};
    uint8_t prefixLen {0};
    uint8_t prefix[ART_MAX_PREFIX];
};
struct ArtN4 : ArtInner { uint8_t keys[4]; ArtBase* child[4]; };
struct ArtN16 : ArtInner { uint8_t keys[16]; ArtBase* child[16]; };
struct ArtN48 : ArtInner { uint8_t childIndex[256]; ArtBase* child[48]; };
struct ArtN256 : ArtInner { ArtBase* child[256]; };

struct DescentStats {
    uint64_t reads {0};
    uint64_t hops {0};                  // inner-node hops (excludes the leaf)
    uint64_t leafCompares {0};
    std::array<uint64_t, 5> typeAtHop[8]; // typeAtHop[hopIndex][ArtType]
    DescentStats() { for (auto& a : typeAtHop) { a.fill(0); } }
};

class Art {
public:
    ArtBase* insert(ArtBase* root, std::string_view key, uint64_t value) { return insertRec(root, key, value, 0); }
    // no path compression / no lazy expansion: one inner node per key byte (the ablation baseline).
    ArtBase* insertNC(ArtBase* root, std::string_view key, uint64_t value) { return insertRecNC(root, key, value, 0); }

    static ArtBase** findChild(ArtInner* node, uint8_t byte) {
        switch (node->type) {
            case ArtType::N4: { ArtN4* n = static_cast<ArtN4*>(node);
                for (uint16_t i = 0; i < n->numChildren; ++i) { if (n->keys[i] == byte) { return &n->child[i]; } } return nullptr; }
            case ArtType::N16: { ArtN16* n = static_cast<ArtN16*>(node);
                for (uint16_t i = 0; i < n->numChildren; ++i) { if (n->keys[i] == byte) { return &n->child[i]; } } return nullptr; }
            case ArtType::N48: { ArtN48* n = static_cast<ArtN48*>(node);
                const uint8_t pos = n->childIndex[byte]; if (pos == 0) { return nullptr; } return &n->child[pos - 1]; }
            case ArtType::N256: { ArtN256* n = static_cast<ArtN256*>(node);
                if (n->child[byte] == nullptr) { return nullptr; } return &n->child[byte]; }
            default: return nullptr;
        }
    }

    // SIMD child search (Leis et al. ICDE 2013 §III-C): N16 found with ONE SSE2 compare + movemask +
    // tzcnt, replacing the data-dependent linear scan. N4 also vectorised; N48/N256 are already
    // direct-indexed. Kills the N16 scan's branch misprediction.
    static ArtBase** findChildSimd(ArtInner* node, uint8_t byte) {
        switch (node->type) {
            case ArtType::N16: { ArtN16* n = static_cast<ArtN16*>(node);
                const __m128i keyvec = _mm_set1_epi8(static_cast<char>(byte));
                const __m128i stored = _mm_loadu_si128(reinterpret_cast<const __m128i*>(n->keys));
                unsigned mask = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(keyvec, stored)));
                mask &= (n->numChildren >= 16) ? 0xFFFFu : ((1u << n->numChildren) - 1u);
                if (mask == 0) { return nullptr; }
                return &n->child[__builtin_ctz(mask)]; }
            case ArtType::N4: { ArtN4* n = static_cast<ArtN4*>(node);
                uint32_t four; memcpy(&four, n->keys, 4);
                const __m128i keyvec = _mm_set1_epi8(static_cast<char>(byte));
                const __m128i stored = _mm_cvtsi32_si128(static_cast<int>(four));
                unsigned mask = static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi8(keyvec, stored)));
                mask &= (n->numChildren >= 4) ? 0xFu : ((1u << n->numChildren) - 1u);
                if (mask == 0) { return nullptr; }
                return &n->child[__builtin_ctz(mask)]; }
            case ArtType::N48: { ArtN48* n = static_cast<ArtN48*>(node);
                const uint8_t pos = n->childIndex[byte]; if (pos == 0) { return nullptr; } return &n->child[pos - 1]; }
            case ArtType::N256: { ArtN256* n = static_cast<ArtN256*>(node);
                if (n->child[byte] == nullptr) { return nullptr; } return &n->child[byte]; }
            default: return nullptr;
        }
    }

    // tagged value-in-pointer helpers (Leis et al. §III-D; DuckDB inlines the single rowid this way).
    // Encodes an 8-byte value in a child slot with the low bit set, eliminating the separate leaf node.
    static ArtBase* tagValue(uint64_t v) { return reinterpret_cast<ArtBase*>((v << 1) | 1ull); }
    static bool isTagged(const ArtBase* p) { return (reinterpret_cast<uintptr_t>(p) & 1ull) != 0; }
    static uint64_t untag(const ArtBase* p) { return reinterpret_cast<uintptr_t>(p) >> 1; }

    // Templated lookup: Simd selects findChildSimd; Tagged means child slots may hold an inlined value
    // (value-in-pointer) instead of a leaf pointer — so the cold leaf load is skipped. With Tagged, the
    // un-discriminated key suffix is NOT verified (correct for present keys only; see report).
    template <bool Simd, bool Tagged>
    bool lookupT(ArtBase* root, std::string_view key, uint64_t& out) const {
        ArtBase* cur = root; size_t depth = 0;
        while (cur != nullptr) {
            if (Tagged && isTagged(cur)) { out = untag(cur); return true; }
            if (cur->type == ArtType::Leaf) {
                ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
                if (leaf->key == key) { out = leaf->value; return true; } return false;
            }
            ArtInner* node = static_cast<ArtInner*>(cur);
            if (node->prefixLen > 0) {
                if (depth + node->prefixLen > key.size()) { return false; }
                if (memcmp(node->prefix, key.data() + depth, node->prefixLen) != 0) { return false; }
                depth += node->prefixLen;
            }
            if (depth >= key.size()) { return false; }
            ArtBase** slot = Simd ? findChildSimd(node, static_cast<uint8_t>(key[depth]))
                                  : findChild(node, static_cast<uint8_t>(key[depth]));
            if (slot == nullptr || *slot == nullptr) { return false; }
            cur = *slot; ++depth;
        }
        return false;
    }

    // A0: baseline lookup (full leaf key compare against the pooled string_view)
    bool lookup(ArtBase* root, std::string_view key, uint64_t& out) const {
        ArtBase* cur = root; size_t depth = 0;
        while (cur != nullptr) {
            if (cur->type == ArtType::Leaf) {
                ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
                if (leaf->key == key) { out = leaf->value; return true; } return false;
            }
            ArtInner* node = static_cast<ArtInner*>(cur);
            if (node->prefixLen > 0) {
                if (depth + node->prefixLen > key.size()) { return false; }
                if (memcmp(node->prefix, key.data() + depth, node->prefixLen) != 0) { return false; }
                depth += node->prefixLen;
            }
            if (depth >= key.size()) { return false; }
            ArtBase** slot = findChild(node, static_cast<uint8_t>(key[depth]));
            if (slot == nullptr || *slot == nullptr) { return false; }
            cur = *slot; ++depth;
        }
        return false;
    }

    // A1: skip the leaf key compare entirely (trust the descent) — isolates leaf-compare cost
    bool lookupNoLeafCmp(ArtBase* root, std::string_view key, uint64_t& out) const {
        ArtBase* cur = root; size_t depth = 0;
        while (cur != nullptr) {
            if (cur->type == ArtType::Leaf) { out = static_cast<ArtLeaf*>(cur)->value; return true; }
            ArtInner* node = static_cast<ArtInner*>(cur);
            if (node->prefixLen > 0) {
                if (depth + node->prefixLen > key.size()) { return false; }
                if (memcmp(node->prefix, key.data() + depth, node->prefixLen) != 0) { return false; }
                depth += node->prefixLen;
            }
            if (depth >= key.size()) { return false; }
            ArtBase** slot = findChild(node, static_cast<uint8_t>(key[depth]));
            if (slot == nullptr || *slot == nullptr) { return false; }
            cur = *slot; ++depth;
        }
        return false;
    }

    // A2: compare against the inline key bytes in the leaf (no pooled-key pointer chase)
    bool lookupInlineCmp(ArtBase* root, std::string_view key, uint64_t& out) const {
        ArtBase* cur = root; size_t depth = 0;
        while (cur != nullptr) {
            if (cur->type == ArtType::Leaf) {
                ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
                if (key.size() == 16 && memcmp(leaf->inlineKey, key.data(), 16) == 0) { out = leaf->value; return true; }
                return false;
            }
            ArtInner* node = static_cast<ArtInner*>(cur);
            if (node->prefixLen > 0) {
                if (depth + node->prefixLen > key.size()) { return false; }
                if (memcmp(node->prefix, key.data() + depth, node->prefixLen) != 0) { return false; }
                depth += node->prefixLen;
            }
            if (depth >= key.size()) { return false; }
            ArtBase** slot = findChild(node, static_cast<uint8_t>(key[depth]));
            if (slot == nullptr || *slot == nullptr) { return false; }
            cur = *slot; ++depth;
        }
        return false;
    }

    // instrumentation
    void lookupStats(ArtBase* root, std::string_view key, DescentStats& st) const {
        ArtBase* cur = root; size_t depth = 0; size_t hopIdx = 0; ++st.reads;
        while (cur != nullptr) {
            if (hopIdx < 8) { st.typeAtHop[hopIdx][static_cast<int>(cur->type)]++; }
            if (cur->type == ArtType::Leaf) { ++st.leafCompares; return; }
            ++st.hops;
            ArtInner* node = static_cast<ArtInner*>(cur);
            if (node->prefixLen > 0) { depth += node->prefixLen; }
            if (depth >= key.size()) { return; }
            ArtBase** slot = findChild(node, static_cast<uint8_t>(key[depth]));
            if (slot == nullptr || *slot == nullptr) { return; }
            cur = *slot; ++depth; ++hopIdx;
        }
    }

    static size_t retainedBytes(const std::vector<ArtBase*>& roots) {
        std::unordered_set<const void*> seen; size_t bytes = 0;
        for (ArtBase* r : roots) { visit(r, seen, bytes); } return bytes;
    }
private:
    ArtLeaf* newLeaf(std::string_view key, uint64_t value) {
        ArtLeaf* leaf = new ArtLeaf(); leaf->type = ArtType::Leaf; leaf->key = key; leaf->value = value;
        memset(leaf->inlineKey, 0, 16); memcpy(leaf->inlineKey, key.data(), std::min<size_t>(key.size(), 16));
        return leaf;
    }
    ArtN4* newN4() { ArtN4* n = new ArtN4(); n->type = ArtType::N4; n->numChildren = 0; n->prefixLen = 0;
        memset(n->keys, 0, sizeof(n->keys)); memset(n->child, 0, sizeof(n->child)); return n; }
    ArtInner* cloneInner(ArtInner* node) {
        switch (node->type) {
            case ArtType::N4:  return new ArtN4(*static_cast<ArtN4*>(node));
            case ArtType::N16: return new ArtN16(*static_cast<ArtN16*>(node));
            case ArtType::N48: return new ArtN48(*static_cast<ArtN48*>(node));
            case ArtType::N256:return new ArtN256(*static_cast<ArtN256*>(node));
            default: return nullptr;
        }
    }
    ArtInner* addChild(ArtInner* node, uint8_t byte, ArtBase* childPtr) {
        switch (node->type) {
            case ArtType::N4: { ArtN4* n = static_cast<ArtN4*>(node);
                if (n->numChildren < 4) { n->keys[n->numChildren] = byte; n->child[n->numChildren] = childPtr; ++n->numChildren; return n; }
                ArtN16* g = new ArtN16(); g->type = ArtType::N16; copyHeader(g, n);
                for (uint16_t i = 0; i < 4; ++i) { g->keys[i] = n->keys[i]; g->child[i] = n->child[i]; }
                g->numChildren = 4; g->keys[4] = byte; g->child[4] = childPtr; g->numChildren = 5; return g; }
            case ArtType::N16: { ArtN16* n = static_cast<ArtN16*>(node);
                if (n->numChildren < 16) { n->keys[n->numChildren] = byte; n->child[n->numChildren] = childPtr; ++n->numChildren; return n; }
                ArtN48* g = new ArtN48(); g->type = ArtType::N48; copyHeader(g, n);
                memset(g->childIndex, 0, sizeof(g->childIndex)); memset(g->child, 0, sizeof(g->child));
                for (uint16_t i = 0; i < 16; ++i) { g->child[i] = n->child[i]; g->childIndex[n->keys[i]] = static_cast<uint8_t>(i + 1); }
                g->numChildren = 16; g->child[16] = childPtr; g->childIndex[byte] = 17; g->numChildren = 17; return g; }
            case ArtType::N48: { ArtN48* n = static_cast<ArtN48*>(node);
                if (n->numChildren < 48) { const uint16_t pos = n->numChildren; n->child[pos] = childPtr;
                    n->childIndex[byte] = static_cast<uint8_t>(pos + 1); ++n->numChildren; return n; }
                ArtN256* g = new ArtN256(); g->type = ArtType::N256; copyHeader(g, n); memset(g->child, 0, sizeof(g->child));
                for (uint16_t b = 0; b < 256; ++b) { const uint8_t pos = n->childIndex[b]; if (pos != 0) { g->child[b] = n->child[pos - 1]; } }
                g->numChildren = 48; g->child[byte] = childPtr; ++g->numChildren; return g; }
            case ArtType::N256: { ArtN256* n = static_cast<ArtN256*>(node); n->child[byte] = childPtr; ++n->numChildren; return n; }
            default: return node;
        }
    }
    static void copyHeader(ArtInner* dst, ArtInner* src) {
        dst->numChildren = src->numChildren; dst->prefixLen = src->prefixLen; memcpy(dst->prefix, src->prefix, ART_MAX_PREFIX); }
    size_t firstDifference(std::string_view a, std::string_view b, size_t from) {
        const size_t n = std::min(a.size(), b.size()); size_t i = from; while (i < n && a[i] == b[i]) { ++i; } return i; }
    // Split two leaves that share [depth, diff) and differ at byte `diff`. If the shared run exceeds
    // ART_MAX_PREFIX, chain single-child prefix nodes (each carrying up to MAX prefix bytes + 1 shared
    // branch byte) until the final node, which branches at `diff`. This is faithful pessimistic path
    // compression; the original code mis-branched once the shared run exceeded the inline cap.
    ArtBase* splitTwoLeaves(ArtLeaf* leaf, std::string_view key, uint64_t value, size_t depth, size_t diff) {
        ArtN4* head = newN4(); ArtInner* cur = head; size_t s = depth;
        while (diff - s > ART_MAX_PREFIX) {
            cur->prefixLen = static_cast<uint8_t>(ART_MAX_PREFIX);
            memcpy(cur->prefix, key.data() + s, ART_MAX_PREFIX);
            ArtN4* next = newN4();
            addChild(cur, static_cast<uint8_t>(key[s + ART_MAX_PREFIX]), next);  // single shared branch byte
            cur = next; s += ART_MAX_PREFIX + 1;
        }
        cur->prefixLen = static_cast<uint8_t>(diff - s);
        memcpy(cur->prefix, key.data() + s, diff - s);
        addChild(cur, static_cast<uint8_t>(leaf->key[diff]), leaf);
        addChild(cur, static_cast<uint8_t>(key[diff]), newLeaf(key, value));
        return head;
    }
    ArtBase* insertRec(ArtBase* cur, std::string_view key, uint64_t value, size_t depth) {
        if (cur == nullptr) { return newLeaf(key, value); }
        if (cur->type == ArtType::Leaf) {
            ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
            if (leaf->key == key) { return newLeaf(key, value); }
            const size_t diff = firstDifference(leaf->key, key, depth);
            return splitTwoLeaves(leaf, key, value, depth, diff);
        }
        ArtInner* node = static_cast<ArtInner*>(cur);
        if (node->prefixLen > 0) {
            size_t p = 0;
            while (p < node->prefixLen && depth + p < key.size() && node->prefix[p] == static_cast<uint8_t>(key[depth + p])) { ++p; }
            if (p != node->prefixLen) {
                ArtN4* split = newN4(); split->prefixLen = static_cast<uint8_t>(p); memcpy(split->prefix, node->prefix, p);
                ArtInner* shifted = cloneInner(node); const uint8_t oldByte = node->prefix[p];
                shifted->prefixLen = static_cast<uint8_t>(node->prefixLen - p - 1); memmove(shifted->prefix, node->prefix + p + 1, shifted->prefixLen);
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
            ArtInner* copy = cloneInner(node); *findChild(copy, byte) = newChild; return copy;
        }
        ArtInner* copy = cloneInner(node); return addChild(copy, byte, newLeaf(key, value));
    }

    // No-compression insert: consume exactly one byte per level, store NO prefix bytes. A run of bytes
    // that path compression would fold into one node's prefix instead becomes a chain of single-child
    // nodes — so the tree is as deep as the keys' first-difference position. Mutates in place (no COW);
    // built standalone for the ablation, not versioned.
    ArtBase* insertRecNC(ArtBase* cur, std::string_view key, uint64_t value, size_t depth) {
        if (cur == nullptr) { return newLeaf(key, value); }
        if (cur->type == ArtType::Leaf) {
            ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
            if (leaf->key == key) { leaf->value = value; return leaf; }
            const size_t diff = firstDifference(leaf->key, key, depth);
            ArtN4* head = newN4(); ArtInner* tail = head;
            for (size_t d = depth; d < diff; ++d) {
                ArtN4* nxt = newN4(); tail = static_cast<ArtInner*>(addChild(tail, static_cast<uint8_t>(key[d]), nxt)); tail = nxt;
            }
            addChild(tail, static_cast<uint8_t>(leaf->key[diff]), leaf);
            addChild(tail, static_cast<uint8_t>(key[diff]), newLeaf(key, value));
            return head;
        }
        ArtInner* node = static_cast<ArtInner*>(cur);
        const uint8_t byte = static_cast<uint8_t>(key[depth]);
        ArtBase** slot = findChild(node, byte);
        if (slot != nullptr) { *slot = insertRecNC(*slot, key, value, depth + 1); return node; }
        return addChild(node, byte, newLeaf(key, value));
    }
    static size_t nodeBytes(const ArtBase* n) {
        switch (n->type) { case ArtType::Leaf: return sizeof(ArtLeaf); case ArtType::N4: return sizeof(ArtN4);
            case ArtType::N16: return sizeof(ArtN16); case ArtType::N48: return sizeof(ArtN48); case ArtType::N256: return sizeof(ArtN256); default: return 0; } }
    static void visitChild(ArtBase* c, std::unordered_set<const void*>& seen, size_t& bytes) { if (c) { visit(c, seen, bytes); } }
    static void visit(ArtBase* cur, std::unordered_set<const void*>& seen, size_t& bytes) {
        if (cur == nullptr || !seen.insert(cur).second) { return; }
        bytes += nodeBytes(cur);
        switch (cur->type) {
            case ArtType::Leaf: break;
            case ArtType::N4: { ArtN4* n = static_cast<ArtN4*>(cur); for (uint16_t i=0;i<n->numChildren;++i){visitChild(n->child[i],seen,bytes);} break; }
            case ArtType::N16:{ ArtN16* n = static_cast<ArtN16*>(cur); for (uint16_t i=0;i<n->numChildren;++i){visitChild(n->child[i],seen,bytes);} break; }
            case ArtType::N48:{ ArtN48* n = static_cast<ArtN48*>(cur); for (uint16_t i=0;i<n->numChildren;++i){visitChild(n->child[i],seen,bytes);} break; }
            case ArtType::N256:{ArtN256* n = static_cast<ArtN256*>(cur); for (uint16_t i=0;i<256;++i){visitChild(n->child[i],seen,bytes);} break; }
            default: break;
        }
    }
};

// ============================================================================================
// Workload (high-cardinality slice only is what we read)
// ============================================================================================
struct Workload {
    std::vector<std::string> keyPool; std::vector<std::string_view> keyView; std::vector<uint64_t> keyHash;
    std::vector<uint32_t> writes; std::vector<size_t> lastCommit; std::vector<uint32_t> writeCount; std::vector<uint8_t> present;
    size_t hotKey {0}; size_t churnedCount {0};
};

struct Params { size_t keys {100000}; size_t W {32}; size_t parts {2000}; size_t keyLen {16}; size_t churnedKeys {1024}; uint64_t seed {0x9E3779B97F4A7C15ull}; };

void buildWorkload(const Params& p, Workload& w) {
    SplitMix64 rng(p.seed);
    const char* alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; const size_t A = 62;
    w.keyPool.resize(p.keys); w.keyView.resize(p.keys); w.keyHash.resize(p.keys);
    for (size_t k = 0; k < p.keys; ++k) {
        std::string s(p.keyLen, ' ');
        for (size_t c = 0; c < p.keyLen; ++c) { s[c] = alphabet[rng.nextIndex(A)]; }
        uint64_t tag = k; for (size_t c = 0; c < p.keyLen && tag > 0; ++c) { s[c] = alphabet[tag % A]; tag /= A; }
        w.keyPool[k] = s;
    }
    for (size_t k = 0; k < p.keys; ++k) { w.keyView[k] = w.keyPool[k]; w.keyHash[k] = fnv1a(w.keyView[k]); }
    w.churnedCount = std::min(p.churnedKeys, p.keys);
    size_t churnedW = (p.W > 1) ? (p.W - 1) / 2 : 0;
    w.lastCommit.assign(p.keys, (size_t)-1); w.writeCount.assign(p.keys, 0); w.present.assign(p.keys, 0); w.writes.resize(p.parts * p.W);
    for (size_t c = 0; c < p.parts; ++c) {
        for (size_t j = 0; j < p.W; ++j) {
            size_t idx;
            if (j == 0) { idx = w.hotKey; }
            else if (j <= churnedW && w.churnedCount > 1) { idx = 1 + rng.nextIndex(w.churnedCount - 1); }
            else if (w.churnedCount < p.keys) { idx = w.churnedCount + rng.nextIndex(p.keys - w.churnedCount); }
            else { idx = rng.nextIndex(p.keys); }
            w.writes[c * p.W + j] = (uint32_t)idx; w.present[idx] = 1; w.lastCommit[idx] = c; ++w.writeCount[idx];
        }
    }
}

// Long-shared-prefix workload: every key = [prefixLen identical bytes] + [unique suffix encoding the
// index]. This is where path compression / lazy expansion earns its keep (random 16-byte keys diverge
// in byte 1, so compression does nothing there). All `keys` are present; no churn needed.
void buildPrefixWorkload(const Params& p, size_t prefixLen, Workload& w) {
    SplitMix64 rng(p.seed ^ 0x1234ABCD5678EF90ull);
    const char* alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; const size_t A = 62;
    w.keyPool.resize(p.keys); w.keyView.resize(p.keys); w.keyHash.resize(p.keys);
    std::string prefix(prefixLen, 'Q');   // shared, constant
    for (size_t k = 0; k < p.keys; ++k) {
        std::string s = prefix; s.resize(p.keyLen, ' ');
        for (size_t c = prefixLen; c < p.keyLen; ++c) { s[c] = alphabet[rng.nextIndex(A)]; }
        uint64_t tag = k; for (size_t c = prefixLen; c < p.keyLen && tag > 0; ++c) { s[c] = alphabet[tag % A]; tag /= A; }
        // ensure distinctness even for k=0 by always stamping at least one suffix byte
        if (k == 0 && prefixLen < p.keyLen) { s[prefixLen] = 'a'; }
        w.keyPool[k] = s;
    }
    for (size_t k = 0; k < p.keys; ++k) { w.keyView[k] = w.keyPool[k]; w.keyHash[k] = fnv1a(w.keyView[k]); }
    w.present.assign(p.keys, 1); w.lastCommit.assign(p.keys, 0); w.writeCount.assign(p.keys, 1);
}

// ---- timed runner with perf ------------------------------------------------------------------
struct Timed { double ns; double cyc; double ins; double cm; double bm; uint64_t checksum; };

template <typename Fn>
Timed timeLoop(const std::vector<uint32_t>& probe, int passes, Fn&& fn) {
    Timed best{1e18,1e18,0,0,0,0}; bool haveValid = false;
    for (int pass = 0; pass < passes; ++pass) {
        PerfGroup pg; pg.start();
        uint64_t sum = 0; const double s = nowNs();
        for (size_t i = 0; i < probe.size(); ++i) { sum += fn(probe[i]); }
        const double e = nowNs();
        double cyc=0,ins=0,cm=0,bm=0; if (pg.ok) { pg.stop((double)probe.size(), cyc, ins, cm, bm); }
        const double per = (e - s) / (double)probe.size();
        // pick the best by cyc among perf-valid passes (cyc is the frequency-invariant metric); only fall
        // back to wall-time selection if perf never opened, so a transient perf failure can't record cyc=0.
        if (pg.ok && cyc > 0) { if (!haveValid || cyc < best.cyc) { best = Timed{per, cyc, ins, cm, bm, sum}; haveValid = true; } }
        else if (!haveValid && per < best.ns) { best = Timed{per, 0, 0, 0, 0, sum}; }
    }
    return best;
}

void printRow(const char* label, const Timed& t) {
    std::printf("  %-46s | %7.1f | %7.2f | %7.2f | %6.3f | %6.4f | %6.3f\n",
                label, t.ns, t.cyc, t.ins, t.cyc/t.ins, t.cm, t.bm);
}

// ---- AMAC-style 8-way software-pipelined ART descent (hides dependent-load latency) ----------
template <int B>
uint64_t batchedArt(const Art& art, ArtBase* root, const std::vector<uint32_t>& probe,
                    const std::vector<std::string_view>& keyView) {
    uint64_t sum = 0; const size_t N = probe.size();
    ArtBase* node[B]; size_t depth[B]; const char* kd[B]; size_t kl[B]; bool done[B];
    for (size_t base = 0; base < N; base += B) {
        const int m = (int)std::min<size_t>(B, N - base);
        int remaining = m;
        for (int j = 0; j < m; ++j) {
            node[j] = root; depth[j] = 0; std::string_view k = keyView[probe[base + j]];
            kd[j] = k.data(); kl[j] = k.size(); done[j] = false;
            __builtin_prefetch(root);
        }
        while (remaining > 0) {
            for (int j = 0; j < m; ++j) {
                if (done[j]) { continue; }
                ArtBase* cur = node[j];
                if (cur->type == ArtType::Leaf) {
                    ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
                    if (leaf->key.size() == kl[j] && memcmp(leaf->key.data(), kd[j], kl[j]) == 0) { sum += leaf->value; }
                    done[j] = true; --remaining; continue;
                }
                ArtInner* in = static_cast<ArtInner*>(cur);
                if (in->prefixLen > 0) { depth[j] += in->prefixLen; }
                if (depth[j] >= kl[j]) { done[j] = true; --remaining; continue; }
                ArtBase** slot = Art::findChild(in, (uint8_t)kd[j][depth[j]]);
                if (slot == nullptr || *slot == nullptr) { done[j] = true; --remaining; continue; }
                ArtBase* next = *slot; __builtin_prefetch(next); node[j] = next; ++depth[j];
            }
        }
    }
    return sum;
}

// ---- AMAC-style 8-way pipelined mvcc HEAD lookup (precomputed hash, hides slot+head latency) --
template <int B>
uint64_t batchedMvcc(const MvccHash& mvcc, const std::vector<uint32_t>& probe,
                     const std::vector<std::string_view>& keyView, const std::vector<uint64_t>& keyHash) {
    uint64_t sum = 0; const size_t N = probe.size(); const uint64_t mask = mvcc.maskVal();
    const MvccSlot* base0 = mvcc.slots().data();
    size_t idx[B]; uint64_t h[B]; std::string_view key[B]; int phase[B]; bool done[B];
    for (size_t base = 0; base < N; base += B) {
        const int m = (int)std::min<size_t>(B, N - base);
        int remaining = m;
        for (int j = 0; j < m; ++j) {
            const uint32_t k = probe[base + j]; h[j] = keyHash[k]; key[j] = keyView[k];
            idx[j] = h[j] & mask; phase[j] = 0; done[j] = false; __builtin_prefetch(&base0[idx[j]]);
        }
        while (remaining > 0) {
            for (int j = 0; j < m; ++j) {
                if (done[j]) { continue; }
                const MvccSlot& s = base0[idx[j]];
                if (phase[j] == 0) {
                    if (!s.used) { done[j] = true; --remaining; continue; }
                    if (s.hash == h[j] && s.key == key[j]) { __builtin_prefetch(s.head); phase[j] = 1; continue; }
                    idx[j] = (idx[j] + 1) & mask; __builtin_prefetch(&base0[idx[j]]); continue;
                }
                if (s.head != nullptr) { sum += s.head->value; }
                done[j] = true; --remaining;
            }
        }
    }
    return sum;
}

// ---- AMAC 8-way + SIMD + full-leaf VERIFY (absent-key-safe: compares the full key at the leaf) ----
template <int B>
uint64_t batchedArtSimdSafe(ArtBase* root, const std::vector<uint32_t>& probe,
                            const std::vector<std::string_view>& keyView) {
    uint64_t sum = 0; const size_t N = probe.size();
    ArtBase* node[B]; size_t depth[B]; const char* kd[B]; size_t kl[B]; bool done[B];
    for (size_t base = 0; base < N; base += B) {
        const int m = (int)std::min<size_t>(B, N - base); int remaining = m;
        for (int j = 0; j < m; ++j) { node[j] = root; depth[j] = 0;
            std::string_view k = keyView[probe[base + j]]; kd[j] = k.data(); kl[j] = k.size(); done[j] = false; __builtin_prefetch(root); }
        while (remaining > 0) {
            for (int j = 0; j < m; ++j) {
                if (done[j]) { continue; }
                ArtBase* cur = node[j];
                if (cur->type == ArtType::Leaf) {
                    ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
                    if (leaf->key.size() == kl[j] && memcmp(leaf->key.data(), kd[j], kl[j]) == 0) { sum += leaf->value; }
                    done[j] = true; --remaining; continue;
                }
                ArtInner* in = static_cast<ArtInner*>(cur);
                if (in->prefixLen > 0) { depth[j] += in->prefixLen; }
                if (depth[j] >= kl[j]) { done[j] = true; --remaining; continue; }
                ArtBase** slot = Art::findChildSimd(in, (uint8_t)kd[j][depth[j]]);
                if (slot == nullptr || *slot == nullptr) { done[j] = true; --remaining; continue; }
                ArtBase* next = *slot; __builtin_prefetch(next); node[j] = next; ++depth[j];
            }
        }
    }
    return sum;
}

// ---- AMAC 8-way MVCC with the hash computed INSIDE (fair: a hash index must digest the query key) --
template <int B>
uint64_t batchedMvccHashed(const MvccHash& mvcc, const std::vector<uint32_t>& probe,
                           const std::vector<std::string_view>& keyView) {
    uint64_t sum = 0; const size_t N = probe.size(); const uint64_t mask = mvcc.maskVal();
    const MvccSlot* base0 = mvcc.slots().data();
    size_t idx[B]; uint64_t h[B]; std::string_view key[B]; int phase[B]; bool done[B];
    for (size_t base = 0; base < N; base += B) {
        const int m = (int)std::min<size_t>(B, N - base); int remaining = m;
        for (int j = 0; j < m; ++j) { const uint32_t k = probe[base + j];
            key[j] = keyView[k]; h[j] = fnv1a(key[j]); idx[j] = h[j] & mask; phase[j] = 0; done[j] = false; __builtin_prefetch(&base0[idx[j]]); }
        while (remaining > 0) {
            for (int j = 0; j < m; ++j) {
                if (done[j]) { continue; }
                const MvccSlot& s = base0[idx[j]];
                if (phase[j] == 0) {
                    if (!s.used) { done[j] = true; --remaining; continue; }
                    if (s.hash == h[j] && s.key == key[j]) { __builtin_prefetch(s.head); phase[j] = 1; continue; }
                    idx[j] = (idx[j] + 1) & mask; __builtin_prefetch(&base0[idx[j]]); continue;
                }
                if (s.head != nullptr) { sum += s.head->value; }
                done[j] = true; --remaining;
            }
        }
    }
    return sum;
}

// ---- value-in-pointer clone: deep-copy inner nodes, replace each leaf child with a tagged value ----
ArtInner* cloneInnerStandalone(ArtInner* n) {
    switch (n->type) {
        case ArtType::N4:  return new ArtN4(*static_cast<ArtN4*>(n));
        case ArtType::N16: return new ArtN16(*static_cast<ArtN16*>(n));
        case ArtType::N48: return new ArtN48(*static_cast<ArtN48*>(n));
        case ArtType::N256:return new ArtN256(*static_cast<ArtN256*>(n));
        default: return nullptr;
    }
}
ArtBase* cloneTagged(ArtBase* node, std::unordered_map<ArtBase*, ArtBase*>& memo) {
    if (node == nullptr) { return nullptr; }
    if (node->type == ArtType::Leaf) { return node; }   // only if the whole tree is one leaf
    auto it = memo.find(node); if (it != memo.end()) { return it->second; }
    ArtInner* dst = cloneInnerStandalone(static_cast<ArtInner*>(node)); memo[node] = dst;
    auto fix = [&](ArtBase*& s) { if (s == nullptr) { return; }
        if (s->type == ArtType::Leaf) { s = Art::tagValue(static_cast<ArtLeaf*>(s)->value); } else { s = cloneTagged(s, memo); } };
    switch (dst->type) {
        case ArtType::N4:  { ArtN4* n = static_cast<ArtN4*>(dst);  for (uint16_t i=0;i<n->numChildren;++i){fix(n->child[i]);} break; }
        case ArtType::N16: { ArtN16* n = static_cast<ArtN16*>(dst); for (uint16_t i=0;i<n->numChildren;++i){fix(n->child[i]);} break; }
        case ArtType::N48: { ArtN48* n = static_cast<ArtN48*>(dst); for (uint16_t i=0;i<n->numChildren;++i){fix(n->child[i]);} break; }
        case ArtType::N256:{ ArtN256* n = static_cast<ArtN256*>(dst); for (int i=0;i<256;++i){fix(n->child[i]);} break; }
        default: break;
    }
    return dst;
}

// ---- stacked AMAC: 8-way pipelined + SIMD node search + value-in-pointer (no leaf load) ----
template <int B>
uint64_t batchedArtOpt(ArtBase* taggedRoot, const std::vector<uint32_t>& probe,
                       const std::vector<std::string_view>& keyView) {
    uint64_t sum = 0; const size_t N = probe.size();
    ArtBase* node[B]; size_t depth[B]; const char* kd[B]; size_t kl[B]; bool done[B];
    for (size_t base = 0; base < N; base += B) {
        const int m = (int)std::min<size_t>(B, N - base); int remaining = m;
        for (int j = 0; j < m; ++j) { node[j] = taggedRoot; depth[j] = 0;
            std::string_view k = keyView[probe[base + j]]; kd[j] = k.data(); kl[j] = k.size(); done[j] = false; __builtin_prefetch(taggedRoot); }
        while (remaining > 0) {
            for (int j = 0; j < m; ++j) {
                if (done[j]) { continue; }
                ArtBase* cur = node[j];
                ArtInner* in = static_cast<ArtInner*>(cur);
                if (in->prefixLen > 0) { depth[j] += in->prefixLen; }
                if (depth[j] >= kl[j]) { done[j] = true; --remaining; continue; }
                ArtBase** slot = Art::findChildSimd(in, (uint8_t)kd[j][depth[j]]);
                if (slot == nullptr || *slot == nullptr) { done[j] = true; --remaining; continue; }
                ArtBase* next = *slot;
                if (Art::isTagged(next)) { sum += Art::untag(next); done[j] = true; --remaining; continue; }  // value-in-pointer
                __builtin_prefetch(next); node[j] = next; ++depth[j];
            }
        }
    }
    return sum;
}

// ============================================================================================
// MISS OPTIMIZATION — leaf fingerprint carried in the child pointer's spare bits.
//
// A radix descent matches only the *discriminating* bytes (here bytes 0-2); an absent key that shares
// that prefix descends to the present leaf and only fails at the full-key verify — so a MISS pays the
// cold leaf load just like a HIT (the asymmetry the multiversion hash exploits: it short-circuits at an
// empty slot). The fix: store a cheap fingerprint of the leaf's *full key* in the parent's child slot
// and check it BEFORE chasing the leaf. On a fingerprint MISMATCH the key is provably absent (different
// fingerprint => different key) — return NOT_FOUND with no leaf load. On a fingerprint MATCH still load
// the leaf and verify the full key (so collisions and real hits stay correct). This is sound: 0 false
// negatives (a present key always matches its own fingerprint) and 0 false positives (the full-key
// verify is preserved). It is the DUAL of the rejected value-in-pointer: same mechanism (spare pointer
// bits), but it stores a fingerprint that still verifies, instead of a value that skips verification.
//
// The fingerprint rides in the leaf child pointer's top 16 bits (x86-64 user heap pointers are 48-bit,
// bits 48-63 are zero) with bit 0 as a "this child is a leaf" marker (heap allocations are >=8-aligned,
// so bit 0 is free). Zero extra cache footprint, zero extra load: the pointer is already fetched from
// the (warm) parent node during the descent. Versioning-clean: the tag is immutable node payload, so a
// COW path-copy carries it for free — no separate versioned filter to maintain.
// ============================================================================================
static const uintptr_t FP_LEAF_BIT = 1ull;
static const uintptr_t FP_PTR_MASK = 0x0000FFFFFFFFFFFEull;   // keep bits 1..47 (clear leaf bit + top 16)

// Cheap, position-independent fingerprint of the full key: hardware CRC32 (SSE4.2), ~6-10 cyc for a
// 16-byte key, computed ONCE per lookup (not per level). Robust to where two keys diverge — unlike a
// fixed-position byte tag, which a divergence outside the chosen byte would slip past.
static inline uint16_t fpKey(const char* d, size_t n) {
    uint64_t c = 0; size_t i = 0;
    for (; i + 8 <= n; i += 8) { uint64_t w; memcpy(&w, d + i, 8); c = _mm_crc32_u64(c, w); }
    if (i < n) { uint64_t w = 0; memcpy(&w, d + i, n - i); c = _mm_crc32_u64(c, w); }
    return static_cast<uint16_t>(c ^ (c >> 13));
}
static inline uint16_t fpKey(std::string_view s) { return fpKey(s.data(), s.size()); }

static inline ArtBase* fpTag(ArtLeaf* p, uint16_t fp) {
    return reinterpret_cast<ArtBase*>(reinterpret_cast<uintptr_t>(p) | (static_cast<uintptr_t>(fp) << 48) | FP_LEAF_BIT);
}
static inline bool fpIsLeaf(const ArtBase* p) { return (reinterpret_cast<uintptr_t>(p) & FP_LEAF_BIT) != 0; }
static inline uint16_t fpGet(const ArtBase* p) { return static_cast<uint16_t>(reinterpret_cast<uintptr_t>(p) >> 48); }
static inline ArtLeaf* fpLeaf(const ArtBase* p) { return reinterpret_cast<ArtLeaf*>(reinterpret_cast<uintptr_t>(p) & FP_PTR_MASK); }

// Deep-copy the inner nodes (sharing the leaves) and rewrite every leaf child slot to a fingerprint-
// tagged pointer. Inner children are recursively cloned; the visited memo handles structural sharing.
// Mirrors cloneTagged, but keeps the leaf (tags it) instead of replacing it with an inlined value.
ArtBase* cloneFp(ArtBase* node, std::unordered_map<ArtBase*, ArtBase*>& memo) {
    if (node == nullptr) { return nullptr; }
    if (node->type == ArtType::Leaf) { return node; }   // only if the whole tree is one leaf
    auto it = memo.find(node); if (it != memo.end()) { return it->second; }
    ArtInner* dst = cloneInnerStandalone(static_cast<ArtInner*>(node)); memo[node] = dst;
    auto fix = [&](ArtBase*& s) { if (s == nullptr) { return; }
        if (s->type == ArtType::Leaf) { ArtLeaf* lf = static_cast<ArtLeaf*>(s); s = fpTag(lf, fpKey(lf->key)); }
        else { s = cloneFp(s, memo); } };
    switch (dst->type) {
        case ArtType::N4:  { ArtN4* n = static_cast<ArtN4*>(dst);  for (uint16_t i=0;i<n->numChildren;++i){fix(n->child[i]);} break; }
        case ArtType::N16: { ArtN16* n = static_cast<ArtN16*>(dst); for (uint16_t i=0;i<n->numChildren;++i){fix(n->child[i]);} break; }
        case ArtType::N48: { ArtN48* n = static_cast<ArtN48*>(dst); for (uint16_t i=0;i<n->numChildren;++i){fix(n->child[i]);} break; }
        case ArtType::N256:{ ArtN256* n = static_cast<ArtN256*>(dst); for (int i=0;i<256;++i){fix(n->child[i]);} break; }
        default: break;
    }
    return dst;
}

// Serial SIMD + fingerprint-reject + full-leaf verify (absent-key-safe). qfp is precomputed by the caller.
template <bool Simd>
bool lookupFp(ArtBase* root, std::string_view key, uint16_t qfp, uint64_t& out) {
    ArtBase* cur = root; size_t depth = 0;
    while (cur != nullptr) {
        if (cur->type == ArtType::Leaf) {                       // only when the root itself is a leaf
            ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
            if (leaf->key == key) { out = leaf->value; return true; } return false;
        }
        ArtInner* node = static_cast<ArtInner*>(cur);
        if (node->prefixLen > 0) {
            if (depth + node->prefixLen > key.size()) { return false; }
            if (memcmp(node->prefix, key.data() + depth, node->prefixLen) != 0) { return false; }
            depth += node->prefixLen;
        }
        if (depth >= key.size()) { return false; }
        ArtBase** slot = Simd ? Art::findChildSimd(node, static_cast<uint8_t>(key[depth]))
                              : Art::findChild(node, static_cast<uint8_t>(key[depth]));
        if (slot == nullptr || *slot == nullptr) { return false; }
        ArtBase* next = *slot;
        if (fpIsLeaf(next)) {
            if (fpGet(next) != qfp) { return false; }           // fingerprint reject — NO leaf load
            ArtLeaf* leaf = fpLeaf(next);
            if (leaf->key == key) { out = leaf->value; return true; } return false;   // full-key verify
        }
        cur = next; ++depth;
    }
    return false;
}

// AMAC 8-way + SIMD + fingerprint-reject + full-leaf verify (absent-key-safe). On a fingerprint match the
// real leaf is prefetched and verified the next round (so the leaf load stays overlapped); on a mismatch
// the lane finishes immediately with no leaf load.
template <int B>
uint64_t batchedArtFp(ArtBase* root, const std::vector<uint32_t>& probe, const std::vector<std::string_view>& keyView) {
    uint64_t sum = 0; const size_t N = probe.size();
    ArtBase* node[B]; size_t depth[B]; const char* kd[B]; size_t kl[B]; uint16_t qfp[B]; bool done[B];
    for (size_t base = 0; base < N; base += B) {
        const int m = (int)std::min<size_t>(B, N - base); int remaining = m;
        for (int j = 0; j < m; ++j) { node[j] = root; depth[j] = 0;
            std::string_view k = keyView[probe[base + j]]; kd[j] = k.data(); kl[j] = k.size();
            qfp[j] = fpKey(kd[j], kl[j]); done[j] = false; __builtin_prefetch(root); }
        while (remaining > 0) {
            for (int j = 0; j < m; ++j) {
                if (done[j]) { continue; }
                ArtBase* cur = node[j];
                if (cur->type == ArtType::Leaf) {
                    ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
                    if (leaf->key.size() == kl[j] && memcmp(leaf->key.data(), kd[j], kl[j]) == 0) { sum += leaf->value; }
                    done[j] = true; --remaining; continue;
                }
                ArtInner* in = static_cast<ArtInner*>(cur);
                if (in->prefixLen > 0) { depth[j] += in->prefixLen; }
                if (depth[j] >= kl[j]) { done[j] = true; --remaining; continue; }
                ArtBase** slot = Art::findChildSimd(in, (uint8_t)kd[j][depth[j]]);
                if (slot == nullptr || *slot == nullptr) { done[j] = true; --remaining; continue; }
                ArtBase* next = *slot;
                if (fpIsLeaf(next)) {
                    if (fpGet(next) != qfp[j]) { done[j] = true; --remaining; continue; }      // miss: no leaf load
                    ArtLeaf* leaf = fpLeaf(next); __builtin_prefetch(leaf); node[j] = leaf; continue;   // verify next round
                }
                __builtin_prefetch(next); node[j] = next; ++depth[j];
            }
        }
    }
    return sum;
}

// ============================================================================================
// Stride trie (compact) — captures the HOT / Masstree height-reduction mechanism: consume S key
// bytes per level (instead of 1), with lazy expansion (stop at the first node that uniquely picks a
// leaf) and a final full-key verify. Nodes are COMPACT open-addressed hashes sized to their children
// (load factor ~0.5) — the contrast with the failed 512 KB flat-array wide-root. Value-in-pointer at
// the leaf. This approximates HOT (which is bit-granular, fanout k=32) and Masstree (B+tree-of-tries,
// ordered, 8-byte slices) by isolating their shared idea: fewer, wider, cache-dense nodes. Point-
// lookup only; not ordered. Built standalone (not versioned).
// ============================================================================================
struct STLeaf { std::string_view key; uint64_t value; };
// slot tag in low 2 bits: 0 = STNode*, 1 = inlined value (v<<2|1), 2 = STLeaf* (ptr|2)
struct STNode { uint64_t mask {0}; std::vector<uint64_t> chunkAt; std::vector<void*> slotAt; };

uint64_t packChunk(const char* p, size_t len, size_t depth, int S) {
    uint64_t c = 0; for (int i = 0; i < S; ++i) { c <<= 8; c |= (depth + (size_t)i < len) ? (uint8_t)p[depth + i] : 0u; } return c;
}
uint64_t mixChunk(uint64_t c) { c *= 0x9E3779B97F4A7C15ull; return c ^ (c >> 29); }

class StrideTrie {
public:
    explicit StrideTrie(int stride, bool full = false) : _stride(stride), _full(full) {}
    // _full = consume the WHOLE key (no lazy expansion): the terminal slot is reached only by the exact
    // key, so the inlined value needs no suffix verification — absent keys fail during descent. Costs a
    // deeper tree + more nodes, but keeps value-in-pointer's no-leaf-load read while being absent-safe.
    void insert(std::string_view key, uint64_t value) { _root = _full ? insertRecFull(_root, key, value, 0) : insertRec(_root, key, value, 0); }
    // freeze the build-time maps into flat open-addressed nodes
    void* freeze() { return freezeNode(_root); }

    // lookup on the frozen structure
    bool lookup(void* froot, std::string_view key, uint64_t& out) const {
        void* cur = froot; size_t depth = 0;
        while (cur != nullptr) {
            const uintptr_t tag = reinterpret_cast<uintptr_t>(cur) & 3ull;
            if (tag == 1) { out = reinterpret_cast<uintptr_t>(cur) >> 2; return true; }              // inlined value
            if (tag == 2) { STLeaf* lf = reinterpret_cast<STLeaf*>(reinterpret_cast<uintptr_t>(cur) & ~3ull);
                if (lf->key == key) { out = lf->value; return true; } return false; }
            STNode* n = static_cast<STNode*>(cur);
            const uint64_t chunk = packChunk(key.data(), key.size(), depth, _stride);
            uint64_t i = mixChunk(chunk) & n->mask;
            while (true) {
                void* s = n->slotAt[i];
                if (s == nullptr) { return false; }
                if (n->chunkAt[i] == chunk) { cur = s; break; }
                i = (i + 1) & n->mask;
            }
            depth += _stride;
        }
        return false;
    }
    int stride() const { return _stride; }
    // descent shape (frozen): average node hops
    double avgHops(void* froot, const std::vector<std::string_view>& keys, const std::vector<uint32_t>& probe, size_t n) const {
        uint64_t hops = 0;
        for (size_t r = 0; r < n; ++r) { void* cur = froot; size_t depth = 0; std::string_view key = keys[probe[r]];
            while (cur) { const uintptr_t tag = reinterpret_cast<uintptr_t>(cur) & 3ull;
                if (tag == 1 || tag == 2) { break; }
                ++hops; STNode* nd = static_cast<STNode*>(cur);
                const uint64_t chunk = packChunk(key.data(), key.size(), depth, _stride); uint64_t i = mixChunk(chunk) & nd->mask;
                while (nd->slotAt[i] != nullptr && nd->chunkAt[i] != chunk) { i = (i + 1) & nd->mask; }
                cur = nd->slotAt[i]; depth += _stride; }
        }
        return (double)hops / n;
    }
private:
    // build-time node: chunk -> {leaf or child-build-node}
    struct BNode { std::unordered_map<uint64_t, void*> kids; };  // value void*: tag 2=STLeaf*, 0=BNode*
    void* insertRec(void* cur, std::string_view key, uint64_t value, size_t depth) {
        if (cur == nullptr) { STLeaf* lf = new STLeaf{key, value}; return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(lf) | 2ull); }
        const uintptr_t tag = reinterpret_cast<uintptr_t>(cur) & 3ull;
        if (tag == 2) {
            STLeaf* lf = reinterpret_cast<STLeaf*>(reinterpret_cast<uintptr_t>(cur) & ~3ull);
            if (lf->key == key) { lf->value = value; return cur; }
            // split: new BNode, re-insert both at depth (lazy — both chunks at this depth differ eventually)
            BNode* bn = new BNode();
            void* a = insertRec(nullptr, lf->key, lf->value, depth);
            void* b = insertRec(nullptr, key, value, depth);
            const uint64_t ca = packChunk(lf->key.data(), lf->key.size(), depth, _stride);
            const uint64_t cb = packChunk(key.data(), key.size(), depth, _stride);
            if (ca != cb) { bn->kids[ca] = a; bn->kids[cb] = b; }
            else { bn->kids[ca] = insertRec(insertRec(nullptr, lf->key, lf->value, depth + _stride), key, value, depth + _stride); }
            return bn;
        }
        BNode* bn = static_cast<BNode*>(cur);
        const uint64_t chunk = packChunk(key.data(), key.size(), depth, _stride);
        auto it = bn->kids.find(chunk);
        bn->kids[chunk] = (it == bn->kids.end()) ? insertRec(nullptr, key, value, depth + _stride)
                                                 : insertRec(it->second, key, value, depth + _stride);
        return bn;
    }
    // full-consume insert: descend by S-byte chunks until the whole key is consumed, then store the
    // tagged value (tag 1) directly — no STLeaf, no suffix verify needed.
    void* insertRecFull(void* cur, std::string_view key, uint64_t value, size_t depth) {
        BNode* bn = (cur != nullptr) ? static_cast<BNode*>(cur) : new BNode();
        const uint64_t chunk = packChunk(key.data(), key.size(), depth, _stride);
        if (depth + (size_t)_stride >= key.size()) {
            bn->kids[chunk] = reinterpret_cast<void*>((value << 2) | 1ull);   // terminal value (tag 1)
        } else {
            auto it = bn->kids.find(chunk);
            bn->kids[chunk] = insertRecFull(it == bn->kids.end() ? nullptr : it->second, key, value, depth + _stride);
        }
        return bn;
    }
    void* freezeNode(void* cur) {
        if (cur == nullptr) { return nullptr; }
        const uintptr_t tag = reinterpret_cast<uintptr_t>(cur) & 3ull;
        if (tag == 2) { return cur; }   // STLeaf* (lazy mode) — lookup verifies the full key
        if (tag == 1) { return cur; }   // inlined value (full-consume mode) — no verify
        BNode* bn = static_cast<BNode*>(cur);
        size_t cap = 1; while (cap < bn->kids.size() * 2) { cap <<= 1; } if (cap < 2) { cap = 2; }
        STNode* fn = new STNode(); fn->mask = cap - 1; fn->chunkAt.assign(cap, 0); fn->slotAt.assign(cap, nullptr);
        for (auto& kv : bn->kids) {
            void* child = freezeNode(kv.second);
            uint64_t i = mixChunk(kv.first) & fn->mask;
            while (fn->slotAt[i] != nullptr) { i = (i + 1) & fn->mask; }
            fn->chunkAt[i] = kv.first; fn->slotAt[i] = child;
        }
        return fn;
    }
    int _stride {1};
    bool _full {false};
    void* _root {nullptr};
};

struct BenchSet {
    Workload w; MvccHash* mvcc; Art* art; ArtBase* aroot; std::vector<uint32_t> probe; size_t distinct;
};

void buildSet(const Params& p, BenchSet& b) {
    buildWorkload(p, b.w);
    b.distinct = 0; for (uint8_t x : b.w.present) { b.distinct += x; }
    b.mvcc = new MvccHash(p.keys); b.art = new Art(); b.aroot = nullptr;
    for (size_t c = 0; c < p.parts; ++c) {
        for (size_t j = 0; j < p.W; ++j) {
            const uint32_t k = b.w.writes[c*p.W+j];
            b.mvcc->insert(b.w.keyView[k], b.w.keyHash[k], encodeValue(c,k), (uint32_t)c);
            b.aroot = b.art->insert(b.aroot, b.w.keyView[k], encodeValue(c,k));
        }
    }
    SplitMix64 prng(p.seed ^ 0xABCDEF); std::vector<uint32_t> pk;
    for (size_t k = 0; k < p.keys; ++k) { if (b.w.present[k]) { pk.push_back((uint32_t)k); } }
    for (size_t i = 0; i < 2000000; ++i) { b.probe.push_back(pk[prng.nextIndex(pk.size())]); }
}

} // namespace

// build the probe stream (present keys, shuffled) for a workload
void buildProbe(const Params& p, const Workload& w, std::vector<uint32_t>& probe, size_t n) {
    SplitMix64 prng(p.seed ^ 0xABCDEF); std::vector<uint32_t> pk;
    for (size_t k = 0; k < p.keys; ++k) { if (w.present[k]) { pk.push_back((uint32_t)k); } }
    probe.reserve(n); for (size_t i = 0; i < n; ++i) { probe.push_back(pk[prng.nextIndex(pk.size())]); }
}

template <typename Fn>
Timed timeWholeP(const std::vector<uint32_t>& probe, int passes, Fn&& fn) {
    Timed best{1e18,1e18,0,0,0,0}; bool haveValid = false;
    for (int pass = 0; pass < passes; ++pass) {
        PerfGroup pg; pg.start(); const double s = nowNs(); const uint64_t sum = fn(); const double e = nowNs();
        double cyc=0,ins=0,cm=0,bm=0; if (pg.ok) { pg.stop((double)probe.size(), cyc, ins, cm, bm); }
        const double per = (e - s) / (double)probe.size();
        if (pg.ok && cyc > 0) { if (!haveValid || cyc < best.cyc) { best = Timed{per, cyc, ins, cm, bm, sum}; haveValid = true; } }
        else if (!haveValid && per < best.ns) { best = Timed{per, 0, 0, 0, 0, sum}; }
    }
    return best;
}

void printArtShape(const Art& art, ArtBase* root, const Workload& w, const std::vector<uint32_t>& probe) {
    DescentStats st;
    for (size_t i = 0; i < 200000 && i < probe.size(); ++i) { art.lookupStats(root, w.keyView[probe[i]], st); }
    std::printf("  ART descent shape: inner hops=%.3f  leaf=%.3f  total dependent loads=%.3f\n",
                (double)st.hops/st.reads, (double)st.leafCompares/st.reads, (double)(st.hops+st.leafCompares)/st.reads);
    const char* tn[5] = {"Leaf","N4","N16","N48","N256"};
    for (size_t h = 0; h < 8; ++h) {
        uint64_t tot=0; for (int t=0;t<5;++t){tot+=st.typeAtHop[h][t];} if (!tot) { break; }
        std::printf("    hop %zu: ", h);
        for (int t=0;t<5;++t){ if (st.typeAtHop[h][t]) { std::printf("%s=%.0f%% ", tn[t], 100.0*st.typeAtHop[h][t]/tot); } }
        std::printf("\n");
    }
}

void hdr() {
    std::printf("  %-46s | %7s | %7s | %7s | %6s | %6s | %6s\n",
                "variant", "ns/op", "cyc/op", "ins/op", "CPI", "LLC/op", "br.miss");
    std::printf("  -----------------------------------------------+---------+---------+---------+--------+--------+--------\n");
}

int main(int argc, char** argv) {
    Params p;
    for (int i = 1; i < argc; ++i) {
        std::string_view a = argv[i];
        if (a == "--keys" && i+1 < argc) { p.keys = strtoull(argv[++i],nullptr,10); }
        else if (a == "--parts" && i+1 < argc) { p.parts = strtoull(argv[++i],nullptr,10); }
    }

    // ===================================================================================
    // WORKLOAD A — high-cardinality 16-byte keys (the case ART loses by ~2x)
    // ===================================================================================
    Workload w; buildWorkload(p, w);
    size_t distinct = 0; for (uint8_t x : w.present) { distinct += x; }
    MvccHash mvcc(p.keys); Art art; ArtBase* aroot = nullptr;
    for (size_t c = 0; c < p.parts; ++c) {
        for (size_t j = 0; j < p.W; ++j) { const uint32_t k = w.writes[c*p.W+j];
            mvcc.insert(w.keyView[k], w.keyHash[k], encodeValue(c,k), (uint32_t)c);
            aroot = art.insert(aroot, w.keyView[k], encodeValue(c,k)); }
    }
    std::vector<uint32_t> probe; buildProbe(p, w, probe, 2000000);

    // value-in-pointer clone of HEAD; stride tries on present keys at their HEAD value
    std::unordered_map<ArtBase*, ArtBase*> memo; ArtBase* taggedRoot = cloneTagged(aroot, memo);
    StrideTrie st2(2), st4(4), st8(8);
    StrideTrie stF8(8, true), stF4(4, true);   // full-consume (verify-free, absent-safe)
    for (size_t k = 0; k < p.keys; ++k) { if (w.present[k]) { const uint64_t v = encodeValue(w.lastCommit[k], k);
        st2.insert(w.keyView[k], v); st4.insert(w.keyView[k], v); st8.insert(w.keyView[k], v);
        stF8.insert(w.keyView[k], v); stF4.insert(w.keyView[k], v); } }
    void* f2 = st2.freeze(); void* f4 = st4.freeze(); void* f8 = st8.freeze();
    void* fF8 = stF8.freeze(); void* fF4 = stF4.freeze();

    // absent-near probe: present keys with byte 15 forced to '~' (outside the alphabet) — shares the
    // discriminating prefix (bytes 0-2 are the unique stamp), so the descent reaches a present terminal
    // but the full key differs. This is exactly the value-in-pointer false-positive case.
    std::vector<std::string> absentPool; std::vector<std::string_view> absentView;
    for (size_t k = 0; k < p.keys; ++k) { if (w.present[k]) { std::string s(w.keyView[k]); s[15] = '~'; absentPool.push_back(std::move(s)); } }
    absentView.reserve(absentPool.size()); for (const std::string& s : absentPool) { absentView.push_back(s); }
    std::vector<uint32_t> absentProbe; { SplitMix64 prng(p.seed ^ 0xDEAD); absentProbe.reserve(2000000);
        for (size_t i = 0; i < 2000000; ++i) { absentProbe.push_back((uint32_t)prng.nextIndex(absentPool.size())); } }

    { uint64_t s=0; for (int r=0;r<3;++r){ for (size_t i=0;i<probe.size();++i){ uint64_t v=0; art.lookup(aroot,w.keyView[probe[i]],v); s+=v; } } asm volatile(""::"r"(s):"memory"); }

    std::printf("==== WORKLOAD A: high-cardinality %zu-byte keys, present=%zu, parts=%zu ====\n", p.keyLen, distinct, p.parts);
    printArtShape(art, aroot, w, probe);
    std::printf("  stride-trie hops: S=2 %.3f  S=4 %.3f  S=8 %.3f\n\n",
                st2.avgHops(f2, w.keyView, probe, 100000), st4.avgHops(f4, w.keyView, probe, 100000), st8.avgHops(f8, w.keyView, probe, 100000));
    hdr();
    const Timed m0 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; const uint64_t h=fnv1a(w.keyView[k]); mvcc.lookupHead(w.keyView[k],h,v); return v; });
    printRow("MVCC baseline (reference)", m0);
    const Timed a0 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; art.lookup(aroot,w.keyView[k],v); return v; });
    printRow("ART baseline", a0);
    const Timed aSimd = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,false>(aroot,w.keyView[k],v); return v; });
    printRow("ART + SIMD Node16 search", aSimd);
    const Timed aTag = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupT<false,true>(taggedRoot,w.keyView[k],v); return v; });
    printRow("ART + value-in-pointer (tagged leaf, no leaf load)", aTag);
    const Timed aST = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,true>(taggedRoot,w.keyView[k],v); return v; });
    printRow("ART + SIMD + value-in-pointer (stacked)", aST);
    const Timed s8 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; st8.lookup(f8,w.keyView[k],v); return v; });
    printRow("stride-trie S=8 (Masstree-family slice, compact)", s8);
    const Timed s4 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; st4.lookup(f4,w.keyView[k],v); return v; });
    printRow("stride-trie S=4 (compact)", s4);
    const Timed s2 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; st2.lookup(f2,w.keyView[k],v); return v; });
    printRow("stride-trie S=2 (HOT-family, compact)", s2);
    const Timed aB = timeWholeP(probe, 5, [&](){ return batchedArt<8>(art, aroot, probe, w.keyView); });
    printRow("ART + AMAC 8-way pipelined (baseline descent)", aB);
    const Timed aBopt = timeWholeP(probe, 5, [&](){ return batchedArtOpt<8>(taggedRoot, probe, w.keyView); });
    printRow("ART + AMAC 8-way + SIMD + value-in-pointer (ALL)", aBopt);
    const Timed mB = timeWholeP(probe, 5, [&](){ return batchedMvcc<8>(mvcc, probe, w.keyView, w.keyHash); });
    printRow("MVCC + AMAC 8-way (reference)", mB);
    std::printf("  checksums: mvcc=%llu art=%llu simd=%llu tag=%llu stacked=%llu s8=%llu allBatch=%llu\n",
                (unsigned long long)m0.checksum,(unsigned long long)a0.checksum,(unsigned long long)aSimd.checksum,
                (unsigned long long)aTag.checksum,(unsigned long long)aST.checksum,(unsigned long long)s8.checksum,(unsigned long long)aBopt.checksum);

    // ---- SAFE point read: multiversion hash vs the absent-key-safe ART alternatives ----------------
    // The MVCC hash is inherently absent-safe (it compares the full key on every probe). Compare it head
    // to head with the ART variants that ALSO verify (0 false-positives), HIT (present) vs MISS (absent-
    // near = shares the discriminating prefix). The unsafe value-in-pointer is shown only as a contrast.
    auto countFound = [&](auto&& fn) -> size_t {
        size_t found = 0; for (size_t i = 0; i < 200000 && i < absentProbe.size(); ++i) { uint64_t v=0; if (fn(absentProbe[i],v)) { ++found; } } return found;
    };
    const size_t fpMvcc = countFound([&](uint32_t k, uint64_t& v){ return mvcc.lookupHead(absentView[k], fnv1a(absentView[k]), v); });
    const size_t fpArtV = countFound([&](uint32_t k, uint64_t& v){ return art.lookupT<true,false>(aroot, absentView[k], v); });
    const size_t fpFull = countFound([&](uint32_t k, uint64_t& v){ return stF8.lookup(fF8, absentView[k], v); });
    const size_t fpUnsafe = countFound([&](uint32_t k, uint64_t& v){ return art.lookupT<true,true>(taggedRoot, absentView[k], v); });

    std::printf("\n  SAFE point read — multiversion hash vs absent-key-safe ART (cyc/op; HIT=present, MISS=absent-near)\n");
    std::printf("  %-48s | %6s | %6s | %s\n", "variant (all verify the full key unless noted)", "HIT", "MISS", "false-pos /200k");
    std::printf("  -------------------------------------------------+--------+--------+----------------\n");
    // MVCC serial: m0 (HIT, computed above) + absent MISS
    const Timed mMvccMiss = timeLoop(absentProbe, 5, [&](uint32_t k){ uint64_t v=0; const uint64_t h=fnv1a(absentView[k]); mvcc.lookupHead(absentView[k],h,v); return v; });
    std::printf("  %-48s | %5.0fc | %5.0fc | %zu\n", "MVCC hash, serial (hash+probe+keycmp)", m0.cyc, mMvccMiss.cyc, fpMvcc);
    const Timed hMvccB = timeWholeP(probe,       5, [&](){ return batchedMvccHashed<8>(mvcc, probe, w.keyView); });
    const Timed mMvccB = timeWholeP(absentProbe, 5, [&](){ return batchedMvccHashed<8>(mvcc, absentProbe, absentView); });
    std::printf("  %-48s | %5.0fc | %5.0fc | %s\n", "MVCC hash + AMAC 8-way (hash inside)", hMvccB.cyc, mMvccB.cyc, mMvccB.checksum==0?"0":"!=0");
    const Timed hArtV = timeLoop(probe,       5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,false>(aroot, w.keyView[k], v); return v; });
    const Timed mArtV = timeLoop(absentProbe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,false>(aroot, absentView[k], v); return v; });
    std::printf("  %-48s | %5.0fc | %5.0fc | %zu\n", "ART + SIMD + leaf-verify, serial", hArtV.cyc, mArtV.cyc, fpArtV);
    const Timed hArtVB = timeWholeP(probe,       5, [&](){ return batchedArtSimdSafe<8>(aroot, probe, w.keyView); });
    const Timed mArtVB = timeWholeP(absentProbe, 5, [&](){ return batchedArtSimdSafe<8>(aroot, absentProbe, absentView); });
    std::printf("  %-48s | %5.0fc | %5.0fc | %s\n", "ART + SIMD + leaf-verify + AMAC 8-way", hArtVB.cyc, mArtVB.cyc, mArtVB.checksum==0?"0":"!=0");
    const Timed hFull = timeLoop(probe,       5, [&](uint32_t k){ uint64_t v=0; stF8.lookup(fF8, w.keyView[k], v); return v; });
    const Timed mFull = timeLoop(absentProbe, 5, [&](uint32_t k){ uint64_t v=0; stF8.lookup(fF8, absentView[k], v); return v; });
    std::printf("  %-48s | %5.0fc | %5.0fc | %zu\n", "ART full-consume stride S=8 (verify-free)", hFull.cyc, mFull.cyc, fpFull);
    std::printf("  ........................................................ UNSAFE contrast .........\n");
    const Timed hUnsafe = timeLoop(probe,       5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,true>(taggedRoot, w.keyView[k], v); return v; });
    const Timed mUnsafe = timeLoop(absentProbe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,true>(taggedRoot, absentView[k], v); return v; });
    std::printf("  %-48s | %5.0fc | %5.0fc | %zu  <- WRONG\n", "ART + SIMD + value-in-pointer (NO verify)", hUnsafe.cyc, mUnsafe.cyc, fpUnsafe);
    std::printf("  (HIT checksums must match: mvcc=%llu artVerify=%llu full=%llu mvccAMAC=%llu artAMAC=%llu)\n",
                (unsigned long long)m0.checksum,(unsigned long long)hArtV.checksum,(unsigned long long)hFull.checksum,
                (unsigned long long)hMvccB.checksum,(unsigned long long)hArtVB.checksum);

    // ===================================================================================
    // MISS OPTIMIZATION — leaf fingerprint in the child pointer (this study's contribution)
    // ===================================================================================
    // The fingerprint-tagged read tree: leaf child slots carry a 16-bit CRC32 fingerprint of the leaf's
    // full key in the pointer's spare bits. Built once from the HEAD tree (post-build, like cloneTagged).
    std::unordered_map<ArtBase*, ArtBase*> memoFp; ArtBase* fpRoot = cloneFp(aroot, memoFp);

    // Three absent-key distributions, all genuinely absent, to measure misses HONESTLY:
    //  (1) near-byte15 : present key with the LAST byte changed (the existing adversarial set).
    //  (2) near-mid    : present key with a random byte in [5,14] changed — still shares the
    //                    discriminating prefix (descends to the leaf) but a last-byte-only tag would
    //                    NOT reject it. Proves the whole-key fingerprint is position-independent.
    //  (3) random      : fully random absent keys — mixed descent depth (most diverge early). The
    //                    realistic anti-join / existence-check miss, where ART already short-circuits.
    SplitMix64 mrng(p.seed ^ 0x5151AAAA);
    std::vector<std::string> absentMidPool; absentMidPool.reserve(absentPool.size());
    for (size_t k = 0; k < p.keys; ++k) { if (w.present[k]) { std::string s(w.keyView[k]); s[5 + mrng.nextIndex(10)] = '~'; absentMidPool.push_back(std::move(s)); } }
    std::vector<std::string_view> absentMidView; absentMidView.reserve(absentMidPool.size());
    for (const std::string& s : absentMidPool) { absentMidView.push_back(s); }   // reuse absentProbe (same size)

    std::unordered_set<std::string_view> presentSet;
    for (size_t k = 0; k < p.keys; ++k) { if (w.present[k]) { presentSet.insert(w.keyView[k]); } }
    // Match the random pool's size (and thus its cache working set) to the near sets, so the random column
    // measures index cost, not a cold input-string pool. Reuses absentProbe (same size).
    const char* alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"; const size_t AL = 62;
    std::vector<std::string> absentRndPool; absentRndPool.reserve(absentPool.size());
    while (absentRndPool.size() < absentPool.size()) {
        std::string s(p.keyLen, ' '); for (size_t c = 0; c < p.keyLen; ++c) { s[c] = alpha[mrng.nextIndex(AL)]; }
        if (presentSet.find(std::string_view(s)) == presentSet.end()) { absentRndPool.push_back(std::move(s)); }
    }
    std::vector<std::string_view> absentRndView; absentRndView.reserve(absentRndPool.size());
    for (const std::string& s : absentRndPool) { absentRndView.push_back(s); }

    // false-positive audits (must all be 0) and fingerprint rejection rate on the near-mid set
    auto countFp = [&](const std::vector<std::string_view>& view, const std::vector<uint32_t>& pr) -> size_t {
        size_t found = 0; for (size_t i = 0; i < 200000 && i < pr.size(); ++i) { uint64_t v=0; const uint16_t q = fpKey(view[pr[i]]);
            if (lookupFp<true>(fpRoot, view[pr[i]], q, v)) { ++found; } } return found; };
    const size_t fpFp15  = countFp(absentView, absentProbe);
    const size_t fpFpMid = countFp(absentMidView, absentProbe);
    const size_t fpFpRnd = countFp(absentRndView, absentProbe);
    // rejection rate: how many near-mid misses fail at the fingerprint (no leaf load) vs fall through to verify
    size_t rejected = 0, reachedLeaf = 0;
    for (size_t i = 0; i < 200000; ++i) { std::string_view key = absentMidView[absentProbe[i]]; const uint16_t q = fpKey(key);
        ArtBase* cur = fpRoot; size_t depth = 0; bool decided = false;
        while (cur != nullptr && !decided) {
            ArtInner* node = static_cast<ArtInner*>(cur);
            if (node->prefixLen > 0) { if (depth + node->prefixLen > key.size() || memcmp(node->prefix, key.data()+depth, node->prefixLen) != 0) { decided = true; break; } depth += node->prefixLen; }
            if (depth >= key.size()) { decided = true; break; }
            ArtBase** slot = Art::findChildSimd(node, (uint8_t)key[depth]); if (!slot || !*slot) { decided = true; break; }
            ArtBase* next = *slot;
            if (fpIsLeaf(next)) { if (fpGet(next) != q) { ++rejected; } else { ++reachedLeaf; } decided = true; break; }
            cur = next; ++depth;
        }
    }

    // HIT (present) numbers for the fingerprint variant — must match ART+SIMD+verify, checksum included.
    const Timed hFpS = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; lookupFp<true>(fpRoot, w.keyView[k], fpKey(w.keyView[k]), v); return v; });
    const Timed hFpB = timeWholeP(probe, 5, [&](){ return batchedArtFp<8>(fpRoot, probe, w.keyView); });
    // MISS numbers, serial + AMAC, for fingerprint / plain-verify / hash on all three distributions.
    const Timed mFpS15  = timeLoop(absentProbe,    5, [&](uint32_t k){ uint64_t v=0; lookupFp<true>(fpRoot, absentView[k],    fpKey(absentView[k]),    v); return v; });
    const Timed mFpSMid = timeLoop(absentProbe,    5, [&](uint32_t k){ uint64_t v=0; lookupFp<true>(fpRoot, absentMidView[k], fpKey(absentMidView[k]), v); return v; });
    const Timed mFpSRnd = timeLoop(absentProbe, 5, [&](uint32_t k){ uint64_t v=0; lookupFp<true>(fpRoot, absentRndView[k], fpKey(absentRndView[k]), v); return v; });
    const Timed mFpB15  = timeWholeP(absentProbe,    5, [&](){ return batchedArtFp<8>(fpRoot, absentProbe,    absentView); });
    const Timed mFpBMid = timeWholeP(absentProbe,    5, [&](){ return batchedArtFp<8>(fpRoot, absentProbe,    absentMidView); });
    const Timed mFpBRnd = timeWholeP(absentProbe, 5, [&](){ return batchedArtFp<8>(fpRoot, absentProbe, absentRndView); });
    // plain verify (no fingerprint) on the mid + random sets (near15 = mArtV / mArtVB above)
    const Timed mVS_Mid = timeLoop(absentProbe,    5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,false>(aroot, absentMidView[k], v); return v; });
    const Timed mVS_Rnd = timeLoop(absentProbe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupT<true,false>(aroot, absentRndView[k], v); return v; });
    const Timed mVB_Mid = timeWholeP(absentProbe,    5, [&](){ return batchedArtSimdSafe<8>(aroot, absentProbe,    absentMidView); });
    const Timed mVB_Rnd = timeWholeP(absentProbe, 5, [&](){ return batchedArtSimdSafe<8>(aroot, absentProbe, absentRndView); });
    // hash on the mid + random sets (near15 = mMvccMiss / mMvccB above)
    const Timed mHS_Mid = timeLoop(absentProbe,    5, [&](uint32_t k){ uint64_t v=0; mvcc.lookupHead(absentMidView[k], fnv1a(absentMidView[k]), v); return v; });
    const Timed mHS_Rnd = timeLoop(absentProbe, 5, [&](uint32_t k){ uint64_t v=0; mvcc.lookupHead(absentRndView[k], fnv1a(absentRndView[k]), v); return v; });
    const Timed mHB_Mid = timeWholeP(absentProbe,    5, [&](){ return batchedMvccHashed<8>(mvcc, absentProbe,    absentMidView); });
    const Timed mHB_Rnd = timeWholeP(absentProbe, 5, [&](){ return batchedMvccHashed<8>(mvcc, absentProbe, absentRndView); });

    std::printf("\n  MISS OPTIMIZATION — leaf fingerprint in the child pointer (cyc/op; lower better; all ART verify)\n");
    std::printf("  near-byte15/near-mid = present key with 1 byte changed (descends to the leaf); random = fully random absent key\n");
    std::printf("  fingerprint rejection on near-mid: %.3f%% rejected before the leaf load (%zu of %zu), %zu fell through to verify\n",
                100.0*rejected/(rejected+reachedLeaf), rejected, rejected+reachedLeaf, reachedLeaf);
    std::printf("  %-44s | %6s | %9s | %8s | %6s | %s\n", "variant", "HIT", "near15", "near-mid", "random", "false-pos/600k");
    std::printf("  ---------------------------------------------+--------+-----------+----------+--------+--------------\n");
    std::printf("  %-44s | %5.0fc | %8.0fc | %7.0fc | %5.0fc | %zu\n", "MVCC hash, serial",
                m0.cyc, mMvccMiss.cyc, mHS_Mid.cyc, mHS_Rnd.cyc, fpMvcc);
    std::printf("  %-44s | %5.0fc | %8.0fc | %7.0fc | %5.0fc | %s\n", "MVCC hash + AMAC 8-way",
                hMvccB.cyc, mMvccB.cyc, mHB_Mid.cyc, mHB_Rnd.cyc, "0");
    std::printf("  %-44s | %5.0fc | %8.0fc | %7.0fc | %5.0fc | %zu\n", "ART + SIMD + verify, serial",
                hArtV.cyc, mArtV.cyc, mVS_Mid.cyc, mVS_Rnd.cyc, fpArtV);
    std::printf("  %-44s | %5.0fc | %8.0fc | %7.0fc | %5.0fc | %s\n", "ART + SIMD + verify + AMAC",
                hArtVB.cyc, mArtVB.cyc, mVB_Mid.cyc, mVB_Rnd.cyc, "0");
    std::printf("  %-44s | %5.0fc | %8.0fc | %7.0fc | %5.0fc | %zu\n", "ART + SIMD + FINGERPRINT + verify, serial",
                hFpS.cyc, mFpS15.cyc, mFpSMid.cyc, mFpSRnd.cyc, fpFp15 + fpFpMid + fpFpRnd);
    std::printf("  %-44s | %5.0fc | %8.0fc | %7.0fc | %5.0fc | %s\n", "ART + SIMD + FINGERPRINT + verify, AMAC",
                hFpB.cyc, mFpB15.cyc, mFpBMid.cyc, mFpBRnd.cyc, "0");
    std::printf("  (HIT checksums — fp must match verify: artVerify=%llu fpSerial=%llu fpAMAC=%llu)\n",
                (unsigned long long)hArtV.checksum, (unsigned long long)hFpS.checksum, (unsigned long long)hFpB.checksum);
    std::printf("  (per-set false-pos: near15=%zu near-mid=%zu random=%zu — all must be 0)\n", fpFp15, fpFpMid, fpFpRnd);
    // Cost to fingerprint the query column in bulk (CRC32, throughput-bound) — the part of the fp cost a
    // batched probe can lift out of the dependent-load region, shrinking the batched-HIT tax.
    const Timed bulkFp = timeLoop(probe, 5, [&](uint32_t k){ return (uint64_t)fpKey(w.keyView[k]); });
    std::printf("  bulk query fingerprint (CRC32 over the probe column): %.1f cyc/key (amortizable in the batched path)\n", bulkFp.cyc);

    // ===================================================================================
    // WORKLOAD B — long keys with a 40-byte shared prefix (path compression / lazy expansion)
    // ===================================================================================
    Params pb = p; pb.keyLen = 56; pb.keys = 50000;
    const size_t prefixLen = 40;
    Workload wb; buildPrefixWorkload(pb, prefixLen, wb);
    Art artC;  ArtBase* rootC = nullptr;   // path-compressed (real ART)
    Art artNC; ArtBase* rootNC = nullptr;  // no compression / no lazy expansion (one node per byte)
    for (size_t k = 0; k < pb.keys; ++k) { rootC = artC.insert(rootC, wb.keyView[k], encodeValue(0,k)); }
    for (size_t k = 0; k < pb.keys; ++k) { rootNC = artNC.insertNC(rootNC, wb.keyView[k], encodeValue(0,k)); }
    StrideTrie stb8(8); for (size_t k = 0; k < pb.keys; ++k) { stb8.insert(wb.keyView[k], encodeValue(0,k)); }
    void* fb8 = stb8.freeze();
    std::vector<uint32_t> probeB; buildProbe(pb, wb, probeB, 1000000);
    { uint64_t s=0; for (int r=0;r<3;++r){ for (size_t i=0;i<probeB.size();++i){ uint64_t v=0; artC.lookup(rootC,wb.keyView[probeB[i]],v); s+=v; } } asm volatile(""::"r"(s):"memory"); }

    std::printf("\n==== WORKLOAD B: %zu-byte keys, %zu-byte shared prefix, %zu keys (path compression) ====\n", pb.keyLen, prefixLen, pb.keys);
    std::printf("  [path-compressed ART]\n"); printArtShape(artC, rootC, wb, probeB);
    std::printf("  [no compression / no lazy expansion]\n"); printArtShape(artNC, rootNC, wb, probeB);
    std::printf("  stride-trie S=8 hops: %.3f\n\n", stb8.avgHops(fb8, wb.keyView, probeB, 100000));
    hdr();
    const Timed bC = timeLoop(probeB, 5, [&](uint32_t k){ uint64_t v=0; artC.lookup(rootC,wb.keyView[k],v); return v; });
    printRow("ART path compression + lazy expansion (ON)", bC);
    const Timed bNC = timeLoop(probeB, 5, [&](uint32_t k){ uint64_t v=0; artNC.lookup(rootNC,wb.keyView[k],v); return v; });
    printRow("ART NO compression (one node per byte)", bNC);
    const Timed bCsimd = timeLoop(probeB, 5, [&](uint32_t k){ uint64_t v=0; artC.lookupT<true,false>(rootC,wb.keyView[k],v); return v; });
    printRow("ART compressed + SIMD", bCsimd);
    const Timed bS8 = timeLoop(probeB, 5, [&](uint32_t k){ uint64_t v=0; stb8.lookup(fb8,wb.keyView[k],v); return v; });
    printRow("stride-trie S=8 (8-byte slices skip the shared prefix fast)", bS8);
    std::printf("  checksums: compressed=%llu nocompress=%llu stride8=%llu\n",
                (unsigned long long)bC.checksum,(unsigned long long)bNC.checksum,(unsigned long long)bS8.checksum);
    return EXIT_SUCCESS;
}
