#include <gtest/gtest.h>

#include "BumpPtrAllocator.h"

#include <stdint.h>
#include <new>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

struct LifecycleCounters {
    size_t ctorCount {0};
    size_t dtorCount {0};
};

class Tracker {
public:
    explicit Tracker(LifecycleCounters* counters)
        : _counters(counters)
    {
        ++_counters->ctorCount;
    }

    Tracker(const Tracker&) = delete;
    Tracker& operator=(const Tracker&) = delete;

    ~Tracker() {
        ++_counters->dtorCount;
    }

private:
    LifecycleCounters* _counters {nullptr};
};

class OrderTracker {
public:
    OrderTracker(std::vector<int>* order, int id)
        : _order(order),
        _id(id)
    {
    }

    OrderTracker(const OrderTracker&) = delete;
    OrderTracker& operator=(const OrderTracker&) = delete;

    ~OrderTracker() {
        _order->push_back(_id);
    }

private:
    std::vector<int>* _order {nullptr};
    int _id {0};
};

struct alignas(64) OverAligned {
    int value {0};
};

struct PodPayload {
    uint64_t a {0};
    uint32_t b {0};
    uint16_t c {0};
    uint8_t d {0};
};

struct TrackedItem {
    LifecycleCounters* counters {nullptr};
    int value {0};

    TrackedItem(LifecycleCounters* c, int v)
        : counters(c),
        value(v)
    {
        ++counters->ctorCount;
    }

    TrackedItem(const TrackedItem& other)
        : counters(other.counters),
        value(other.value)
    {
        ++counters->ctorCount;
    }

    TrackedItem& operator=(const TrackedItem&) = delete;

    ~TrackedItem() {
        ++counters->dtorCount;
    }
};

class VectorHolder {
public:
    VectorHolder() = default;
    VectorHolder(const VectorHolder&) = delete;
    VectorHolder& operator=(const VectorHolder&) = delete;

    void push(int v) {
        _values.push_back(v);
    }

    int sum() const {
        int total = 0;
        for (const int v : _values) {
            total += v;
        }
        return total;
    }

    size_t size() const {
        return _values.size();
    }

private:
    std::vector<int> _values;
};

class StringHolder {
public:
    explicit StringHolder(const char* s)
        : _str(s)
    {
    }

    StringHolder(const StringHolder&) = delete;
    StringHolder& operator=(const StringHolder&) = delete;

    const std::string& str() const {
        return _str;
    }

private:
    std::string _str;
};

class NestedContainerHolder {
public:
    NestedContainerHolder() = default;
    NestedContainerHolder(const NestedContainerHolder&) = delete;
    NestedContainerHolder& operator=(const NestedContainerHolder&) = delete;

    void add(const std::string& key, int value) {
        _map[key].push_back(value);
    }

    size_t totalValues() const {
        size_t total = 0;
        for (const auto& [k, v] : _map) {
            total += v.size();
        }
        return total;
    }

    int sumForKey(const std::string& key) const {
        const auto it = _map.find(key);
        if (it == _map.end()) {
            return 0;
        }
        int total = 0;
        for (const int v : it->second) {
            total += v;
        }
        return total;
    }

private:
    std::unordered_map<std::string, std::vector<int>> _map;
};

class TrackedVectorHolder {
public:
    explicit TrackedVectorHolder(LifecycleCounters* counters)
        : _counters(counters)
    {
    }

    TrackedVectorHolder(const TrackedVectorHolder&) = delete;
    TrackedVectorHolder& operator=(const TrackedVectorHolder&) = delete;

    void add(int v) {
        _items.emplace_back(_counters, v);
    }

    size_t size() const {
        return _items.size();
    }

private:
    LifecycleCounters* _counters {nullptr};
    std::vector<TrackedItem> _items;
};

}

class BumpPtrAllocatorTest : public ::testing::Test {
};

TEST_F(BumpPtrAllocatorTest, AllocateSingle) {
    BumpPtrAllocator alloc(1024);

    void* ptr = alloc.allocate(64, alignof(std::max_align_t));

    ASSERT_NE(ptr, nullptr);
}

TEST_F(BumpPtrAllocatorTest, AllocateMultipleReturnsDistinctPointers) {
    BumpPtrAllocator alloc(4096);

    void* p1 = alloc.allocate(32, 8);
    void* p2 = alloc.allocate(32, 8);
    void* p3 = alloc.allocate(32, 8);

    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);
    ASSERT_NE(p3, nullptr);
    ASSERT_NE(p1, p2);
    ASSERT_NE(p2, p3);
    ASSERT_NE(p1, p3);
}

TEST_F(BumpPtrAllocatorTest, AllocateRespectsAlignment) {
    BumpPtrAllocator alloc(4096);

    const size_t alignments[] = {1, 2, 4, 8, 16, 32, 64, 128};
    for (const size_t align : alignments) {
        alloc.allocate(1, 1);
        void* ptr = alloc.allocate(16, align);
        ASSERT_NE(ptr, nullptr) << "align=" << align;
        const uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
        ASSERT_EQ(addr % align, 0u) << "align=" << align << " addr=" << addr;
    }
}

TEST_F(BumpPtrAllocatorTest, AllocationsDoNotOverlap) {
    BumpPtrAllocator alloc(4096);

    const size_t numAllocs = 200;
    std::vector<uint8_t*> ptrs;
    std::vector<size_t> sizes;
    ptrs.reserve(numAllocs);
    sizes.reserve(numAllocs);

    for (size_t i = 0; i < numAllocs; ++i) {
        const size_t size = 16 + (i % 11);
        uint8_t* p = static_cast<uint8_t*>(alloc.allocate(size, 8));
        ASSERT_NE(p, nullptr);
        const uint8_t pattern = static_cast<uint8_t>((i * 31u + 7u) & 0xff);
        for (size_t j = 0; j < size; ++j) {
            p[j] = pattern;
        }
        ptrs.push_back(p);
        sizes.push_back(size);
    }

    for (size_t i = 0; i < numAllocs; ++i) {
        const uint8_t pattern = static_cast<uint8_t>((i * 31u + 7u) & 0xff);
        for (size_t j = 0; j < sizes[i]; ++j) {
            ASSERT_EQ(ptrs[i][j], pattern) << "i=" << i << " j=" << j;
        }
    }
}

TEST_F(BumpPtrAllocatorTest, AllocateLargerThanDefaultSlab) {
    BumpPtrAllocator alloc(128);

    void* ptr = alloc.allocate(8192, alignof(std::max_align_t));

    ASSERT_NE(ptr, nullptr);
    uint8_t* bytes = static_cast<uint8_t*>(ptr);
    for (size_t i = 0; i < 8192; ++i) {
        bytes[i] = static_cast<uint8_t>(i & 0xff);
    }
    for (size_t i = 0; i < 8192; ++i) {
        ASSERT_EQ(bytes[i], static_cast<uint8_t>(i & 0xff));
    }
}

TEST_F(BumpPtrAllocatorTest, ManyAllocationsAcrossSlabs) {
    BumpPtrAllocator alloc(256);

    const size_t count = 2000;
    std::vector<int*> ints;
    ints.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        int* p = static_cast<int*>(alloc.allocate(sizeof(int), alignof(int)));
        ASSERT_NE(p, nullptr);
        *p = static_cast<int>(i);
        ints.push_back(p);
    }

    for (size_t i = 0; i < count; ++i) {
        ASSERT_EQ(*ints[i], static_cast<int>(i));
    }
}

TEST_F(BumpPtrAllocatorTest, CreateTrivialType) {
    BumpPtrAllocator alloc(1024);

    int* p = alloc.create<int>(42);

    ASSERT_NE(p, nullptr);
    ASSERT_EQ(*p, 42);
}

TEST_F(BumpPtrAllocatorTest, CreatePodPayload) {
    BumpPtrAllocator alloc(1024);

    PodPayload* p = alloc.create<PodPayload>();
    ASSERT_NE(p, nullptr);
    p->a = 0xdeadbeefcafef00dULL;
    p->b = 0x12345678u;
    p->c = 0xabcdu;
    p->d = 0x7fu;

    ASSERT_EQ(p->a, 0xdeadbeefcafef00dULL);
    ASSERT_EQ(p->b, 0x12345678u);
    ASSERT_EQ(p->c, 0xabcdu);
    ASSERT_EQ(p->d, 0x7fu);
}

TEST_F(BumpPtrAllocatorTest, CreateNonTrivialCallsDestructorOnAllocDestruction) {
    LifecycleCounters counters;
    {
        BumpPtrAllocator alloc(1024);
        Tracker* t = alloc.create<Tracker>(&counters);
        ASSERT_NE(t, nullptr);
        ASSERT_EQ(counters.ctorCount, 1u);
        ASSERT_EQ(counters.dtorCount, 0u);
    }
    ASSERT_EQ(counters.dtorCount, 1u);
}

TEST_F(BumpPtrAllocatorTest, CreateManyNonTrivialCallsAllDestructors) {
    const size_t count = 100;
    LifecycleCounters counters;
    {
        BumpPtrAllocator alloc(512);
        for (size_t i = 0; i < count; ++i) {
            Tracker* t = alloc.create<Tracker>(&counters);
            ASSERT_NE(t, nullptr);
        }
        ASSERT_EQ(counters.ctorCount, count);
        ASSERT_EQ(counters.dtorCount, 0u);
    }
    ASSERT_EQ(counters.dtorCount, count);
}

TEST_F(BumpPtrAllocatorTest, DestructorsRunInReverseConstructionOrder) {
    std::vector<int> order;
    {
        BumpPtrAllocator alloc(1024);
        alloc.create<OrderTracker>(&order, 1);
        alloc.create<OrderTracker>(&order, 2);
        alloc.create<OrderTracker>(&order, 3);
        alloc.create<OrderTracker>(&order, 4);
    }
    ASSERT_EQ(order.size(), 4u);
    ASSERT_EQ(order[0], 4);
    ASSERT_EQ(order[1], 3);
    ASSERT_EQ(order[2], 2);
    ASSERT_EQ(order[3], 1);
}

TEST_F(BumpPtrAllocatorTest, CreateWithNoDestructDoesNotCallDestructor) {
    LifecycleCounters counters;
    {
        BumpPtrAllocator alloc(1024);
        Tracker* t = alloc.createWithNoDestruct<Tracker>(&counters);
        ASSERT_NE(t, nullptr);
        ASSERT_EQ(counters.ctorCount, 1u);
    }
    ASSERT_EQ(counters.dtorCount, 0u);
}

TEST_F(BumpPtrAllocatorTest, ResetCallsRegisteredDestructors) {
    LifecycleCounters counters;
    BumpPtrAllocator alloc(1024);

    for (size_t i = 0; i < 10; ++i) {
        alloc.create<Tracker>(&counters);
    }
    ASSERT_EQ(counters.ctorCount, 10u);
    ASSERT_EQ(counters.dtorCount, 0u);

    alloc.reset();

    ASSERT_EQ(counters.dtorCount, 10u);
}

TEST_F(BumpPtrAllocatorTest, ResetAllowsFurtherAllocations) {
    BumpPtrAllocator alloc(1024);

    void* before = alloc.allocate(64, 8);
    ASSERT_NE(before, nullptr);

    alloc.reset();

    void* after = alloc.allocate(64, 8);
    ASSERT_NE(after, nullptr);
}

TEST_F(BumpPtrAllocatorTest, ResetDoesNotCallDestructorsTwice) {
    LifecycleCounters counters;
    {
        BumpPtrAllocator alloc(1024);
        for (size_t i = 0; i < 5; ++i) {
            alloc.create<Tracker>(&counters);
        }
        alloc.reset();
        ASSERT_EQ(counters.dtorCount, 5u);
    }
    ASSERT_EQ(counters.dtorCount, 5u);
}

TEST_F(BumpPtrAllocatorTest, MultipleResetCycles) {
    LifecycleCounters counters;
    BumpPtrAllocator alloc(512);

    const size_t cycles = 5;
    const size_t perCycle = 20;

    for (size_t c = 0; c < cycles; ++c) {
        for (size_t i = 0; i < perCycle; ++i) {
            alloc.create<Tracker>(&counters);
        }
        alloc.reset();
        ASSERT_EQ(counters.ctorCount, (c + 1) * perCycle);
        ASSERT_EQ(counters.dtorCount, (c + 1) * perCycle);
    }
}

TEST_F(BumpPtrAllocatorTest, PlacementNewOperator) {
    BumpPtrAllocator alloc(1024);

    int* p = new (alloc) int(7);

    ASSERT_NE(p, nullptr);
    ASSERT_EQ(*p, 7);
}

TEST_F(BumpPtrAllocatorTest, AlignedPlacementNewOperator) {
    BumpPtrAllocator alloc(4096);

    OverAligned* p = new (std::align_val_t(alignof(OverAligned)), alloc) OverAligned();

    ASSERT_NE(p, nullptr);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(p) % alignof(OverAligned), 0u);
    p->value = 99;
    ASSERT_EQ(p->value, 99);
    p->~OverAligned();
}

TEST_F(BumpPtrAllocatorTest, MixedSizeAndAlignmentStress) {
    BumpPtrAllocator alloc(1024);

    struct Entry {
        uint8_t* ptr {nullptr};
        size_t size {0};
        size_t align {0};
        uint8_t pattern {0};
    };

    std::vector<Entry> entries;
    const size_t count = 500;
    entries.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        Entry e;
        e.size = 1 + (i % 257);
        const size_t alignChoices[] = {1, 2, 4, 8, 16, 32, 64};
        e.align = alignChoices[i % (sizeof(alignChoices) / sizeof(alignChoices[0]))];
        e.pattern = static_cast<uint8_t>((i * 131u + 5u) & 0xff);
        e.ptr = static_cast<uint8_t*>(alloc.allocate(e.size, e.align));
        ASSERT_NE(e.ptr, nullptr);
        ASSERT_EQ(reinterpret_cast<uintptr_t>(e.ptr) % e.align, 0u);
        for (size_t j = 0; j < e.size; ++j) {
            e.ptr[j] = e.pattern;
        }
        entries.push_back(e);
    }

    for (size_t i = 0; i < entries.size(); ++i) {
        const Entry& e = entries[i];
        for (size_t j = 0; j < e.size; ++j) {
            ASSERT_EQ(e.ptr[j], e.pattern) << "i=" << i << " j=" << j;
        }
    }
}

TEST_F(BumpPtrAllocatorTest, EmptyAllocatorDestroysCleanly) {
    BumpPtrAllocator alloc(1024);
}

TEST_F(BumpPtrAllocatorTest, ResetOnEmptyAllocator) {
    BumpPtrAllocator alloc(1024);

    alloc.reset();
    alloc.reset();

    void* p = alloc.allocate(32, 8);
    ASSERT_NE(p, nullptr);
}

TEST_F(BumpPtrAllocatorTest, CreateAfterResetReusesAllocator) {
    LifecycleCounters counters;
    BumpPtrAllocator alloc(1024);

    alloc.create<Tracker>(&counters);
    ASSERT_EQ(counters.ctorCount, 1u);

    alloc.reset();
    ASSERT_EQ(counters.dtorCount, 1u);

    alloc.create<Tracker>(&counters);
    ASSERT_EQ(counters.ctorCount, 2u);
    ASSERT_EQ(counters.dtorCount, 1u);
}

TEST_F(BumpPtrAllocatorTest, ClassWithVectorWorks) {
    BumpPtrAllocator alloc(1024);

    VectorHolder* h = alloc.create<VectorHolder>();
    ASSERT_NE(h, nullptr);

    for (int i = 1; i <= 100; ++i) {
        h->push(i);
    }
    ASSERT_EQ(h->size(), 100u);
    ASSERT_EQ(h->sum(), 5050);
}

TEST_F(BumpPtrAllocatorTest, ClassWithLongStringWorks) {
    const char* longText =
        "This string is intentionally long enough to exceed the small-string "
        "optimization buffer on every standard library implementation, which "
        "forces std::string to allocate its storage on the heap.";

    BumpPtrAllocator alloc(1024);

    StringHolder* h = alloc.create<StringHolder>(longText);
    ASSERT_NE(h, nullptr);
    ASSERT_EQ(h->str(), longText);
    ASSERT_GT(h->str().size(), 64u);
}

TEST_F(BumpPtrAllocatorTest, ClassWithNestedContainersWorks) {
    BumpPtrAllocator alloc(2048);

    NestedContainerHolder* h = alloc.create<NestedContainerHolder>();
    ASSERT_NE(h, nullptr);

    h->add("a", 1);
    h->add("a", 2);
    h->add("a", 3);
    h->add("b", 10);
    h->add("b", 20);
    h->add("c", 100);

    ASSERT_EQ(h->totalValues(), 6u);
    ASSERT_EQ(h->sumForKey("a"), 6);
    ASSERT_EQ(h->sumForKey("b"), 30);
    ASSERT_EQ(h->sumForKey("c"), 100);
    ASSERT_EQ(h->sumForKey("missing"), 0);
}

TEST_F(BumpPtrAllocatorTest, ClassWithVectorOfTrackedItemsDestroyedOnAllocDestruction) {
    LifecycleCounters counters;
    {
        BumpPtrAllocator alloc(1024);
        TrackedVectorHolder* h = alloc.create<TrackedVectorHolder>(&counters);
        ASSERT_NE(h, nullptr);
        for (int i = 0; i < 50; ++i) {
            h->add(i);
        }
        ASSERT_EQ(h->size(), 50u);
        ASSERT_GE(counters.ctorCount, 50u);
        ASSERT_LT(counters.dtorCount, counters.ctorCount);
    }
    ASSERT_EQ(counters.ctorCount, counters.dtorCount);
}

TEST_F(BumpPtrAllocatorTest, ResetRunsClassDestructorAndReleasesSTLContents) {
    LifecycleCounters counters;
    BumpPtrAllocator alloc(1024);

    TrackedVectorHolder* h = alloc.create<TrackedVectorHolder>(&counters);
    for (int i = 0; i < 30; ++i) {
        h->add(i);
    }
    const size_t aliveAfterFill = counters.ctorCount - counters.dtorCount;
    ASSERT_EQ(aliveAfterFill, 30u);

    alloc.reset();

    ASSERT_EQ(counters.ctorCount, counters.dtorCount);
}

TEST_F(BumpPtrAllocatorTest, ManyClassesWithSTLAllReleased) {
    LifecycleCounters counters;
    const size_t holderCount = 20;
    const size_t perHolder = 25;

    {
        BumpPtrAllocator alloc(2048);
        for (size_t i = 0; i < holderCount; ++i) {
            TrackedVectorHolder* h = alloc.create<TrackedVectorHolder>(&counters);
            for (size_t j = 0; j < perHolder; ++j) {
                h->add(static_cast<int>(i * 1000 + j));
            }
        }
        ASSERT_GE(counters.ctorCount, holderCount * perHolder);
    }

    ASSERT_EQ(counters.ctorCount, counters.dtorCount);
}

TEST_F(BumpPtrAllocatorTest, ResetCyclesWithSTLContainersStayBalanced) {
    LifecycleCounters counters;
    BumpPtrAllocator alloc(1024);

    const size_t cycles = 4;
    for (size_t c = 0; c < cycles; ++c) {
        TrackedVectorHolder* h = alloc.create<TrackedVectorHolder>(&counters);
        for (int i = 0; i < 40; ++i) {
            h->add(i);
        }
        alloc.reset();
        ASSERT_EQ(counters.ctorCount, counters.dtorCount) << "cycle=" << c;
    }
}

TEST_F(BumpPtrAllocatorTest, CreateWithNoDestructDoesNotReleaseSTLContents) {
    LifecycleCounters counters;
    {
        BumpPtrAllocator alloc(1024);
        TrackedVectorHolder* h = alloc.createWithNoDestruct<TrackedVectorHolder>(&counters);
        ASSERT_NE(h, nullptr);
        for (int i = 0; i < 10; ++i) {
            h->add(i);
        }
        ASSERT_GE(counters.ctorCount, 10u);
        // Explicit destructor call is required because the holder owns heap
        // memory through its std::vector, which the allocator will not reclaim.
        h->~TrackedVectorHolder();
    }
    ASSERT_EQ(counters.ctorCount, counters.dtorCount);
}
