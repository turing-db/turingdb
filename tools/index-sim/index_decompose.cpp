// decompose.cpp — instrumented decomposition of the high-cardinality, low-lag (HEAD) read for the
// multiversion hash vs ART, to attribute ART's ~2x read latency to specific causes.
//
// Reuses the EXACT structures from index_bench.cpp (MvccHash, Art, workload) so numbers match, and
// adds: (1) ablation read variants that each remove one cost, (2) exact hop / node-type / chain
// instrumentation, (3) inline hardware counters (cycles, instructions, LLC misses, branch misses)
// around each timed loop, (4) an 8-way software-pipelined (MLP) descent to measure latency-bound-ness.
//
// Build: g++ -std=c++23 -O2 -o decompose decompose.cpp
// Run:   taskset -c 0 ./decompose

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/perf_event.h>

#include <string>
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
        leader = perfOpen(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, -1);
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
    ArtBase* insertRec(ArtBase* cur, std::string_view key, uint64_t value, size_t depth) {
        if (cur == nullptr) { return newLeaf(key, value); }
        if (cur->type == ArtType::Leaf) {
            ArtLeaf* leaf = static_cast<ArtLeaf*>(cur);
            if (leaf->key == key) { return newLeaf(key, value); }
            const size_t diff = firstDifference(leaf->key, key, depth);
            ArtN4* node = newN4(); node->prefixLen = static_cast<uint8_t>(std::min(diff - depth, ART_MAX_PREFIX));
            memcpy(node->prefix, key.data() + depth, node->prefixLen);
            ArtInner* filled = addChild(node, static_cast<uint8_t>(leaf->key[diff]), leaf);
            filled = addChild(filled, static_cast<uint8_t>(key[diff]), newLeaf(key, value));
            return filled;
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

// ---- timed runner with perf ------------------------------------------------------------------
struct Timed { double ns; double cyc; double ins; double cm; double bm; uint64_t checksum; };

template <typename Fn>
Timed timeLoop(const std::vector<uint32_t>& probe, int passes, Fn&& fn) {
    Timed best{1e18,0,0,0,0,0};
    for (int pass = 0; pass < passes; ++pass) {
        PerfGroup pg; pg.start();
        uint64_t sum = 0; const double s = nowNs();
        for (size_t i = 0; i < probe.size(); ++i) { sum += fn(probe[i]); }
        const double e = nowNs();
        double cyc=0,ins=0,cm=0,bm=0; if (pg.ok) { pg.stop((double)probe.size(), cyc, ins, cm, bm); }
        const double per = (e - s) / (double)probe.size();
        if (per < best.ns) { best = Timed{per, cyc, ins, cm, bm, sum}; }
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

int main(int argc, char** argv) {
    Params p;
    for (int i = 1; i < argc; ++i) {
        std::string_view a = argv[i];
        if (a == "--key-len" && i+1 < argc) { p.keyLen = strtoull(argv[++i],nullptr,10); }
        else if (a == "--keys" && i+1 < argc) { p.keys = strtoull(argv[++i],nullptr,10); }
        else if (a == "--parts" && i+1 < argc) { p.parts = strtoull(argv[++i],nullptr,10); }
    }
    Workload w; buildWorkload(p, w);
    size_t distinct = 0; for (uint8_t x : w.present) { distinct += x; }

    MvccHash mvcc(p.keys);
    Art art; ArtBase* aroot = nullptr;
    for (size_t c = 0; c < p.parts; ++c) {
        for (size_t j = 0; j < p.W; ++j) {
            const uint32_t k = w.writes[c*p.W+j];
            mvcc.insert(w.keyView[k], w.keyHash[k], encodeValue(c,k), (uint32_t)c);
            aroot = art.insert(aroot, w.keyView[k], encodeValue(c,k));
        }
    }

    // high-cardinality probe = all present keys, shuffled (matches index_bench `probe`)
    std::vector<uint32_t> probe; { SplitMix64 prng(p.seed ^ 0xABCDEF); std::vector<uint32_t> pk;
        for (size_t k = 0; k < p.keys; ++k) { if (w.present[k]) { pk.push_back((uint32_t)k); } }
        for (size_t i = 0; i < 2000000; ++i) { probe.push_back(pk[prng.nextIndex(pk.size())]); } }

    // warmup: ramp turbo before timing (powersave governor starts low)
    { uint64_t s = 0; for (int rep = 0; rep < 3; ++rep) { for (size_t i = 0; i < probe.size(); ++i) {
        uint64_t v=0; art.lookup(aroot, w.keyView[probe[i]], v); s += v; } } asm volatile("" : : "r"(s) : "memory"); }

    // ---- instrumentation: exact descent shape ----
    DescentStats st; size_t mvccProbeSum = 0, mvccChainSum = 0;
    for (size_t i = 0; i < 200000; ++i) {
        art.lookupStats(aroot, w.keyView[probe[i]], st);
        size_t pl, cd; mvcc.lookupStats(w.keyView[probe[i]], w.keyHash[probe[i]], pl, cd);
        mvccProbeSum += pl; mvccChainSum += cd;
    }
    std::printf("workload: keys=%zu present=%zu key-len=%zuB parts=%zu  (high-cardinality HEAD read)\n\n",
                p.keys, distinct, p.keyLen, p.parts);
    std::printf("ART descent shape (avg over 200k reads): inner hops=%.3f  leaf compares=%.3f  total dependent loads=%.3f\n",
                (double)st.hops/st.reads, (double)st.leafCompares/st.reads, (double)(st.hops+st.leafCompares)/st.reads);
    const char* tn[5] = {"Leaf","N4","N16","N48","N256"};
    for (size_t h = 0; h < 5; ++h) {
        uint64_t tot=0; for (int t=0;t<5;++t){tot+=st.typeAtHop[h][t];} if (!tot) break;
        std::printf("   node at hop %zu: ", h);
        for (int t=0;t<5;++t){ if (st.typeAtHop[h][t]) std::printf("%s=%.0f%% ", tn[t], 100.0*st.typeAtHop[h][t]/tot); }
        std::printf("\n");
    }
    std::printf("MVCC probe length avg=%.4f  chain depth avg=%.4f (HEAD)\n\n",
                (double)mvccProbeSum/200000, (double)mvccChainSum/200000);

    std::printf("Per-read decomposition (best of 5 passes, taskset to one core recommended)\n");
    std::printf("  %-40s | %7s | %7s | %7s | %6s | %6s | %6s\n",
                "variant", "ns/op", "cyc/op", "ins/op", "CPI", "LLC/op", "br.miss");
    std::printf("  -----------------------------------------+---------+---------+---------+--------+--------+--------\n");

    // MVCC
    const Timed m0 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; const uint64_t h=fnv1a(w.keyView[k]); mvcc.lookupHead(w.keyView[k],h,v); return v; });
    printRow("MVCC M0 baseline (hash+keycmp inside)", m0);
    const Timed m1 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; mvcc.lookupHead(w.keyView[k],w.keyHash[k],v); return v; });
    printRow("MVCC M1 precomputed hash (no fnv1a)", m1);
    const Timed m2 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; const uint64_t h=fnv1a(w.keyView[k]); mvcc.lookupHeadNoKeyCmp(h,v); return v; });
    printRow("MVCC M2 hash inside, NO key compare", m2);
    const Timed m3 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; mvcc.lookupHeadNoKeyCmp(w.keyHash[k],v); return v; });
    printRow("MVCC M3 precomputed hash, NO key compare", m3);

    // ART
    const Timed a0 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; art.lookup(aroot,w.keyView[k],v); return v; });
    printRow("ART A0 baseline (descent + pooled keycmp)", a0);
    const Timed a1 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupNoLeafCmp(aroot,w.keyView[k],v); return v; });
    printRow("ART A1 descent only (NO leaf compare)", a1);
    const Timed a2 = timeLoop(probe, 5, [&](uint32_t k){ uint64_t v=0; art.lookupInlineCmp(aroot,w.keyView[k],v); return v; });
    printRow("ART A2 inline-key leaf compare (no pool chase)", a2);

    // ---- IDEATION EXPERIMENT 1: wide 2-byte root (collapse the two dense upper N256 hops -> 1) ----
    // The first two key bytes are dense (index stamp), so hops 0,1 are both N256. Replace them with one
    // 65536-entry array indexed by the first two bytes, pointing straight at the real hop-2 subtree.
    std::vector<ArtBase*> root2(65536, nullptr);
    for (size_t k = 0; k < p.keys; ++k) {
        if (!w.present[k]) { continue; }
        std::string_view key = w.keyView[k];
        ArtBase* cur = aroot; size_t depth = 0;
        // descend exactly two byte-levels (root + level1), both N256 with no prefix here
        for (int lvl = 0; lvl < 2 && cur && cur->type != ArtType::Leaf; ++lvl) {
            ArtInner* in = static_cast<ArtInner*>(cur);
            if (in->prefixLen) { depth += in->prefixLen; }
            ArtBase** slot = Art::findChild(in, (uint8_t)key[depth]);
            cur = slot ? *slot : nullptr; ++depth;
        }
        const uint16_t idx = ((uint8_t)key[0] << 8) | (uint8_t)key[1];
        root2[idx] = cur;
    }
    auto lookupWideRoot = [&](uint32_t k) -> uint64_t {
        std::string_view key = w.keyView[k];
        const uint16_t idx = ((uint8_t)key[0] << 8) | (uint8_t)key[1];
        ArtBase* cur = root2[idx]; size_t depth = 2;
        while (cur != nullptr) {
            if (cur->type == ArtType::Leaf) { ArtLeaf* lf = static_cast<ArtLeaf*>(cur);
                if (lf->key == key) { return lf->value; } return 0; }
            ArtInner* in = static_cast<ArtInner*>(cur);
            if (in->prefixLen > 0) { if (depth + in->prefixLen > key.size()) { return 0; } depth += in->prefixLen; }
            if (depth >= key.size()) { return 0; }
            ArtBase** slot = Art::findChild(in, (uint8_t)key[depth]); if (!slot || !*slot) { return 0; }
            cur = *slot; ++depth;
        }
        return 0;
    };
    const Timed aW = timeLoop(probe, 5, lookupWideRoot);
    printRow("ART A-wide 2-byte root (4 hops -> 3)", aW);

    // ---- AMAC 8-way software-pipelined (MLP) variants: is the read latency-bound? ----
    auto timeWhole = [&](auto&& fn) -> Timed {
        Timed best{1e18,0,0,0,0,0};
        for (int pass = 0; pass < 5; ++pass) {
            PerfGroup pg; pg.start(); const double s = nowNs(); const uint64_t sum = fn(); const double e = nowNs();
            double cyc=0,ins=0,cm=0,bm=0; if (pg.ok) { pg.stop((double)probe.size(), cyc, ins, cm, bm); }
            const double per = (e - s) / (double)probe.size();
            if (per < best.ns) { best = Timed{per, cyc, ins, cm, bm, sum}; }
        }
        return best;
    };
    const Timed mB = timeWhole([&](){ return batchedMvcc<8>(mvcc, probe, w.keyView, w.keyHash); });
    printRow("MVCC M-batch 8-way pipelined (precomp hash)", mB);
    const Timed aB = timeWhole([&](){ return batchedArt<8>(art, aroot, probe, w.keyView); });
    printRow("ART  A-batch 8-way pipelined descent", aB);

    std::printf("\nchecksums (sanity): m0=%llu a0=%llu  mBatch=%llu aBatch=%llu\n",
                (unsigned long long)m0.checksum, (unsigned long long)a0.checksum,
                (unsigned long long)mB.checksum, (unsigned long long)aB.checksum);

    // ---- key-length sweep: hashing cost grows with length, ART descent stays flat ----
    std::printf("\nKey-length sweep — why ART loses at 16B but wins long (cycles/op, best of 5):\n");
    std::printf("  %-8s | %-28s | %-28s | %s\n", "key len", "MVCC M0 (hash+probe+cmp)", "ART A0 (descent+cmp)", "hash-only cost (M0-M1)");
    std::printf("  ---------+------------------------------+------------------------------+----------------------\n");
    for (size_t klen : {16, 24, 32, 48, 64, 100}) {
        Params pp = p; pp.keyLen = klen; BenchSet b; buildSet(pp, b);
        const Timed sm0 = timeLoop(b.probe, 5, [&](uint32_t k){ uint64_t v=0; const uint64_t h=fnv1a(b.w.keyView[k]); b.mvcc->lookupHead(b.w.keyView[k],h,v); return v; });
        const Timed sm1 = timeLoop(b.probe, 5, [&](uint32_t k){ uint64_t v=0; b.mvcc->lookupHead(b.w.keyView[k],b.w.keyHash[k],v); return v; });
        Art* bart = b.art; ArtBase* broot = b.aroot; const Workload* bw = &b.w;
        const Timed sa0 = timeLoop(b.probe, 5, [=](uint32_t k){ uint64_t v=0; bart->lookup(broot,bw->keyView[k],v); return v; });
        std::printf("  %5zu B  | %7.1f cyc  %6.1f ns       | %7.1f cyc  %6.1f ns       | %7.1f cyc  %s  (chk a=%llu)\n",
                    klen, sm0.cyc, sm0.ns, sa0.cyc, sa0.ns, sm0.cyc - sm1.cyc,
                    sa0.cyc < sm0.cyc ? "<- ART wins" : "           ", (unsigned long long)sa0.checksum);
        delete b.mvcc; delete b.art;
    }
    return EXIT_SUCCESS;
}
