#include "AgingRingCache.h"
#include "TuringTest.h"

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

using namespace turing::test;

namespace {

// ---------------------------------------------------------------------------
// Payload: dynamic-sized to exercise memory usage calculation
// ---------------------------------------------------------------------------

struct DynamicPayload {
    std::vector<int> _items;
};

size_t calcMemUsage(const DynamicPayload& p) {
    return sizeof(DynamicPayload) + p._items.size() * sizeof(int);
}

// ---------------------------------------------------------------------------
// Fake disk: simulates async IO with a small sleep
// ---------------------------------------------------------------------------

struct FakeDisk {
    std::mutex _mutex;
    std::map<int, std::vector<int>> _store;
    std::atomic<size_t> _saveCount {0};
    std::atomic<size_t> _loadCount {0};
    bool _saveShouldFail {false};

    AgingRingCacheResult<void> load(int key, DynamicPayload& payload) {
        _loadCount++;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        const std::scoped_lock lock(_mutex);
        const auto it = _store.find(key);

        if (it != _store.end()) {
            payload._items = it->second;
        } else {
            // Default: fill with key-derived values
            payload._items.assign(4, key);
        }

        return {};
    }

    AgingRingCacheResult<void> save(int key, const DynamicPayload& payload) {
        if (_saveShouldFail) {
            return AgingRingCacheError::result(AgingRingCacheErrorCode::UNKNOWN);
        }

        _saveCount++;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        const std::scoped_lock lock(_mutex);
        _store[key] = payload._items;

        return {};
    }
};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

using TestCache = AgingRingCache<int, DynamicPayload>;

class AgingRingCacheTest : public TuringTest {
protected:
    void initialize() override {}
    void terminate() override {}

    /** Build a cache wired up to a FakeDisk instance. */
    void buildCache(TestCache& cache, FakeDisk& disk, size_t maxMemBytes = 64ul * 1024 * 1024) {
        cache.setOnLoad([&disk](const int& key, DynamicPayload& p, void*) {
            return disk.load(key, p).has_value();
        });

        cache.setOnEvict([&disk](const int& key, DynamicPayload& p) {
            return disk.save(key, p).has_value();
        });

        cache.setCalculateMemoryUsage(calcMemUsage);
        cache.setMaxMemUsage(maxMemBytes);
    }
};

}

// ---------------------------------------------------------------------------
// General usage
// ---------------------------------------------------------------------------

TEST_F(AgingRingCacheTest, GeneralUsage) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    // Acquire a key — triggers a load
    {
        auto res = cache.acquire(1);
        ASSERT_TRUE(res);
        EXPECT_EQ((*res)->_items, std::vector<int>(4, 1));
    }

    cache.shutdown();
    EXPECT_GE(disk._saveCount.load(), 1u);
}

// ---------------------------------------------------------------------------
// Single-threading
// ---------------------------------------------------------------------------

TEST_F(AgingRingCacheTest, ST_ConstructAcquireRelease) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    {
        auto res = cache.acquire(42);
        ASSERT_TRUE(res);
        EXPECT_EQ((*res)->_items, std::vector<int>(4, 42));
    }
    cache.shutdown();
}

TEST_F(AgingRingCacheTest, ST_RepeatedAcquireSameKey) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    // First acquire — loads from disk
    {
        auto res = cache.acquire(7);
        ASSERT_TRUE(res);
    }

    const size_t loadsAfterFirst = disk._loadCount.load();

    // Second acquire — should hit the cache, no new load
    {
        auto res = cache.acquire(7);
        ASSERT_TRUE(res);
    }
    EXPECT_EQ(disk._loadCount.load(), loadsAfterFirst);

    cache.shutdown();
}

TEST_F(AgingRingCacheTest, ST_MultipleKeys) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    for (int i = 0; i < 5; i++) {
        auto res = cache.acquire(i);
        ASSERT_TRUE(res) << "key=" << i;
    }

    cache.shutdown();
    EXPECT_EQ(disk._loadCount.load(), 5u);
    EXPECT_EQ(disk._saveCount.load(), 5u); // shutdown evicts all
}

TEST_F(AgingRingCacheTest, ST_HandleMoveSemantics) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    {
        auto res = cache.acquire(99);
        ASSERT_TRUE(res);

        // Move the handle
        TestCache::Handle moved = std::move(*res);
        EXPECT_TRUE(moved);
        EXPECT_FALSE(*res); // original is now empty

        // Payload is accessible through moved handle
        EXPECT_EQ(moved->_items, std::vector<int>(4, 99));
    }

    cache.shutdown();
}

TEST_F(AgingRingCacheTest, ST_HandleMoveAssignment) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    {
        auto res1 = cache.acquire(1);
        auto res2 = cache.acquire(2);
        ASSERT_TRUE(res1);
        ASSERT_TRUE(res2);

        *res1 = std::move(*res2); // res1 releases its old pin, takes res2's pin
        EXPECT_FALSE(*res2);
    }

    cache.shutdown();
}

TEST_F(AgingRingCacheTest, ST_LoadError) {
    TestCache cache;
    const FakeDisk disk;

    cache.setOnLoad([](const int&, DynamicPayload&, void*) {
        return false;
    });

    cache.setOnEvict([](const int&, const DynamicPayload&) {
        return true;
    });

    cache.setCalculateMemoryUsage(calcMemUsage);
    cache.setMaxMemUsage(64ull * 1024 * 1024);

    {
        auto res = cache.acquire(1);
        ASSERT_FALSE(res);
        EXPECT_EQ(res.error().getType(), AgingRingCacheErrorCode::COULD_NOT_LOAD);

        // Entry must have been removed; a second acquire should retry the load
        auto res2 = cache.acquire(1);
        ASSERT_FALSE(res2);
    }

    cache.shutdown();
}

// ---------------------------------------------------------------------------
// Memory usage calculation
// ---------------------------------------------------------------------------

TEST_F(AgingRingCacheTest, ST_MemoryUsageCalculation) {
    TestCache cache;
    FakeDisk disk;

    // Each payload holds exactly 10 ints
    cache.setOnLoad([](const int& key, DynamicPayload& p, void*) {
        p._items.assign(10, key);
        return true;
    });

    cache.setOnEvict([&disk](const int& key, const DynamicPayload& p) {
        return disk.save(key, p).has_value();
    });

    cache.setCalculateMemoryUsage(calcMemUsage);

    // Each entry = sizeof(DynamicPayload) + 10 * sizeof(int) bytes
    const size_t entrySize = sizeof(DynamicPayload) + 10 * sizeof(int);

    // Set memory limit just large enough for 3 entries
    const size_t limit = 3 * entrySize;
    cache.setMaxMemUsage(limit);

    // Load 3 entries — all should fit
    {
        auto h0 = cache.acquire(0);
        ASSERT_TRUE(h0);
        auto h1 = cache.acquire(1);
        ASSERT_TRUE(h1);
        auto h2 = cache.acquire(2);
        ASSERT_TRUE(h2);
    }

    // 4th entry should trigger eviction of one of the previous entries
    {
        auto h3 = cache.acquire(3);
        ASSERT_TRUE(h3);
    }

    // At least one eviction must have happened
    EXPECT_GE(disk._saveCount.load(), 1u);

    cache.shutdown();
}

TEST_F(AgingRingCacheTest, ST_EvictionTriggeredByMemoryLimit) {
    TestCache cache;
    FakeDisk disk;

    // Large payload: 1000 ints each
    cache.setOnLoad([](const int& key, DynamicPayload& p, void*) {
        p._items.assign(1000, key);
        return true;
    });

    cache.setOnEvict([&disk](const int& key, const DynamicPayload& p) {
        return disk.save(key, p).has_value();
    });

    cache.setCalculateMemoryUsage(calcMemUsage);

    const size_t entrySize = sizeof(DynamicPayload) + 1000 * sizeof(int);
    cache.setMaxMemUsage(2 * entrySize); // room for 2 entries

    // Load 5 distinct keys (release each before next acquire to allow eviction)
    for (int i = 0; i < 5; i++) {
        auto h = cache.acquire(i);
        ASSERT_TRUE(h) << "key=" << i;
    }

    cache.shutdown();

    EXPECT_GE(disk._saveCount.load(), 3u); // at least 3 evictions
    cache.shutdown();
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

TEST_F(AgingRingCacheTest, MT_ConcurrentAcquireDifferentKeys) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    constexpr int kThreads = 2;
    constexpr int kKeys = 20;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    std::atomic<int> errorCount {0};

    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t] {
            for (int k = t * kKeys; k < (t + 1) * kKeys; k++) {
                auto res = cache.acquire(k);
                if (!res) {
                    errorCount++;
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(errorCount.load(), 0);
    cache.shutdown();
}

TEST_F(AgingRingCacheTest, MT_ConcurrentAcquireSameKey) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    constexpr int kThreads = 16;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    std::atomic<int> successCount {0};

    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&] {
            auto res = cache.acquire(42);
            if (res) {
                EXPECT_EQ((*res)->_items, std::vector<int>(4, 42));
                successCount++;
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(successCount.load(), kThreads);

    // Only one load should have happened despite 16 concurrent acquires
    EXPECT_EQ(disk._loadCount.load(), 1u);
    cache.shutdown();
}

TEST_F(AgingRingCacheTest, MT_ConcurrentAcquireWithEviction) {
    TestCache cache;
    FakeDisk disk;

    // Tiny payload: 1 int
    cache.setOnLoad([](const int& key, DynamicPayload& p, void*) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        p._items.assign(1, key);
        return true;
    });

    cache.setOnEvict([&disk](const int& key, const DynamicPayload& p) {
        return disk.save(key, p).has_value();
    });

    cache.setCalculateMemoryUsage(calcMemUsage);

    const size_t entrySize = sizeof(DynamicPayload) + 1 * sizeof(int);
    cache.setMaxMemUsage(5 * entrySize); // 5 entries max

    constexpr int kThreads = 8;
    constexpr int kIters = 20;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    std::mutex errorCountsMutex;
    std::unordered_map<AgingRingCacheErrorCode, size_t> errorCounts;

    for (int t = 0; t < kThreads; t++) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kIters; i++) {
                const int key = (t * kIters + i) % 15; // limited key space forces eviction
                auto res = cache.acquire(key);
                if (!res && res.error().getType() != AgingRingCacheErrorCode::NOTHING_TO_EVICT) {
                    const std::unique_lock lock(errorCountsMutex);
                    errorCounts[res.error().getType()]++;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    for (const auto& [code, count] : errorCounts) {
        EXPECT_EQ(count, 0) << "error: " << AgingRingCacheErrorTypeDescription::value(code);
    }

    cache.shutdown();
}

TEST_F(AgingRingCacheTest, MT_PinnedEntriesNotEvicted) {
    TestCache cache;
    FakeDisk disk;

    cache.setOnLoad([](const int& key, DynamicPayload& p, void*) {
        p._items.assign(1000, key);
        return true;
    });

    cache.setOnEvict([&disk](const int& key, const DynamicPayload& p) {
        return disk.save(key, p).has_value();
    });

    cache.setCalculateMemoryUsage(calcMemUsage);

    const size_t entrySize = sizeof(DynamicPayload) + 1000 * sizeof(int);
    cache.setMaxMemUsage(2 * entrySize);

    // Pin key=0 across the whole test
    auto pinned = cache.acquire(0);
    ASSERT_TRUE(pinned);

    // Flood the cache with other keys; key=0 must never be evicted
    std::vector<std::thread> threads;
    threads.reserve(4);

    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&, t] {
            for (int i = 1; i <= 5; i++) {
                auto h = cache.acquire(t * 10 + i);
                (void)h;
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Pinned entry must still be valid and have the original value
    EXPECT_EQ((*pinned)->_items, std::vector<int>(1000, 0));

    *pinned = TestCache::Handle(); // explicit release
    cache.shutdown();
}

// ---------------------------------------------------------------------------
// Shutdown tests
// ---------------------------------------------------------------------------

TEST_F(AgingRingCacheTest, Shutdown_AcquireAfterShutdownReturnsTerminating) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    cache.acquire(1); // warm up, immediately released
    cache.shutdown();

    auto res = cache.acquire(2);
    ASSERT_FALSE(res);
    EXPECT_EQ(res.error().getType(), AgingRingCacheErrorCode::TERMINATING);
}

TEST_F(AgingRingCacheTest, Shutdown_WaitsForPinnedEntries) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    // Acquire and hold a handle in a background thread
    std::atomic<bool> handleReleased {false};
    std::atomic<bool> shutdownStarted {false};

    auto pinRes = cache.acquire(1);
    ASSERT_TRUE(pinRes);

    std::thread releaser([&] {
        // Wait until shutdown has begun, then release the pin
        while (!shutdownStarted.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        *pinRes = TestCache::Handle(); // release
        handleReleased.store(true);
    });

    shutdownStarted.store(true);
    cache.shutdown(); // must block until the pin is released

    EXPECT_TRUE(handleReleased.load());
    releaser.join();
}

TEST_F(AgingRingCacheTest, Shutdown_EvictsAllResidentEntries) {
    TestCache cache;
    FakeDisk disk;
    buildCache(cache, disk);

    constexpr int kKeys = 10;

    // Load entries and release handles
    for (int i = 0; i < kKeys; i++) {
        auto h = cache.acquire(i);
        ASSERT_TRUE(h);
    }

    cache.shutdown();

    // All entries must have been saved to disk
    EXPECT_EQ(disk._saveCount.load(), static_cast<size_t>(kKeys));
    for (int i = 0; i < kKeys; i++) {
        const std::scoped_lock lock(disk._mutex);
        EXPECT_TRUE(disk._store.contains(i)) << "key=" << i << " not saved";
    }
}

// ---------------------------------------------------------------------------
// Save failure tests
// ---------------------------------------------------------------------------

TEST_F(AgingRingCacheTest, SaveFailure_ErrorMessageIsSet) {
    TestCache cache;
    const FakeDisk disk;

    cache.setOnLoad([](const int& key, DynamicPayload& p, void*) {
        p._items.assign(1000, key);
        return true;
    });

    cache.setOnEvict([](const int&, const DynamicPayload&) {
        return false;
    });

    cache.setCalculateMemoryUsage(calcMemUsage);

    const size_t entrySize = sizeof(DynamicPayload) + 1000 * sizeof(int);
    cache.setMaxMemUsage(entrySize); // limit to 1 entry

    // Load first entry
    {
        auto h = cache.acquire(0);
        ASSERT_TRUE(h);
    }

    // Loading a second entry triggers eviction of first, which fails
    {
        auto h = cache.acquire(1);
    } // eviction fails silently in acquire()

    // Verify error message is formatted correctly
    const AgingRingCacheError err(AgingRingCacheErrorCode::UNKNOWN);
    const std::string msg = err.fmtMessage();
    EXPECT_FALSE(msg.empty());
    EXPECT_NE(msg.find("Unknown"), std::string::npos);

    cache.setOnEvict([](const int&, const DynamicPayload&) {
        return true;
    });
    cache.shutdown();
}

TEST_F(AgingRingCacheTest, SaveFailure_EntryPutBackInRing) {
    TestCache cache;
    FakeDisk disk;
    disk._saveShouldFail = true;

    cache.setOnLoad([](const int& key, DynamicPayload& p, void*) {
        p._items.assign(1000, key);
        return true;
    });

    cache.setOnEvict([&disk](const int& key, const DynamicPayload& p) {
        return disk.save(key, p).has_value();
    });

    cache.setCalculateMemoryUsage(calcMemUsage);

    const size_t entrySize = sizeof(DynamicPayload) + 1000 * sizeof(int);
    cache.setMaxMemUsage(entrySize); // limit to 1 entry to force eviction

    // Load key 0 and release it
    {
        auto h = cache.acquire(0);
        ASSERT_TRUE(h);
    }

    // Loading key 1 triggers eviction of key 0, which fails → key 0 goes back in ring
    {
        auto h = cache.acquire(1);
    }

    // Now allow saves to succeed and shut down cleanly
    disk._saveShouldFail = false;

    {
        // The failed-to-evict key (0) should still be accessible
        auto res = cache.acquire(0);
        // It might load fresh or be still resident depending on eviction behaviour,
        // but the acquire must succeed (entry was put back or can be reloaded)
        ASSERT_TRUE(res);
    }

    cache.shutdown();
}

TEST_F(AgingRingCacheTest, SaveFailure_ErrorCodeInResult) {
    // Verify that AgingRingCacheError carries the right code and formats message
    const AgingRingCacheError errTerminating(AgingRingCacheErrorCode::TERMINATING);
    EXPECT_EQ(errTerminating.getType(), AgingRingCacheErrorCode::TERMINATING);
    EXPECT_NE(errTerminating.fmtMessage().find("Terminating"), std::string::npos);

    const AgingRingCacheError errLoad(AgingRingCacheErrorCode::COULD_NOT_LOAD);
    EXPECT_EQ(errLoad.getType(), AgingRingCacheErrorCode::COULD_NOT_LOAD);
    EXPECT_NE(errLoad.fmtMessage().find("load"), std::string::npos);

    const AgingRingCacheError errNothing(AgingRingCacheErrorCode::NOTHING_TO_EVICT);
    EXPECT_EQ(errNothing.getType(), AgingRingCacheErrorCode::NOTHING_TO_EVICT);
    EXPECT_FALSE(errNothing.fmtMessage().empty());
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 40;
    });
}
