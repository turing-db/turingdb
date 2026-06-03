#include "gtest/gtest.h"

#include <thread>
#include <atomic>
#include <vector>
#include <string>
#include <string_view>
#include <memory>
#include <algorithm>

#include "ObjectMap.h"

using namespace db;

namespace {

struct TestObject {
    int _value {0};

    static inline int _liveCount {0};

    explicit TestObject(int value)
        : _value(value)
    {
        ++_liveCount;
    }

    ~TestObject() {
        --_liveCount;
    }
};

std::unique_ptr<TestObject> makeObject(int value) {
    return std::make_unique<TestObject>(value);
}

}

class ObjectMapTest : public ::testing::Test {
protected:
    void SetUp() override {
        TestObject::_liveCount = 0;
    }
};

TEST_F(ObjectMapTest, reserveReturnsValid) {
    ObjectMap<TestObject> map;

    auto reservation = map.reserve("a");
    ASSERT_TRUE(reservation.isValid());
}

TEST_F(ObjectMapTest, reserveSameNameTwiceFails) {
    ObjectMap<TestObject> map;

    auto first = map.reserve("a");
    ASSERT_TRUE(first.isValid());

    auto second = map.reserve("a");
    ASSERT_FALSE(second.isValid());
}

TEST_F(ObjectMapTest, getUnknownReturnsNull) {
    ObjectMap<TestObject> map;

    ASSERT_EQ(map.getObject("missing"), nullptr);
}

TEST_F(ObjectMapTest, reservedButNotPublishedIsInvisible) {
    ObjectMap<TestObject> map;

    auto reservation = map.reserve("a");
    ASSERT_TRUE(reservation.isValid());

    // The name is reserved but no object has been published yet,
    // so it must not be visible to readers.
    ASSERT_EQ(map.getObject("a"), nullptr);
}

TEST_F(ObjectMapTest, publishMakesObjectVisible) {
    ObjectMap<TestObject> map;

    auto reservation = map.reserve("a");
    ASSERT_TRUE(reservation.isValid());

    reservation.publish(makeObject(42));

    const auto* slot = map.getObject("a");
    ASSERT_NE(slot, nullptr);
    ASSERT_FALSE(slot->isFree());
    ASSERT_EQ(slot->getObject()->_value, 42);
}

TEST_F(ObjectMapTest, publishInvalidatesReservation) {
    ObjectMap<TestObject> map;

    auto reservation = map.reserve("a");
    reservation.publish(makeObject(1));

    ASSERT_FALSE(reservation.isValid());
}

TEST_F(ObjectMapTest, cancelledReservationReleasesName) {
    ObjectMap<TestObject> map;

    {
        auto reservation = map.reserve("a");
        ASSERT_TRUE(reservation.isValid());
        // Reservation goes out of scope without publishing: the name
        // reservation is cancelled.
    }

    ASSERT_EQ(map.getObject("a"), nullptr);

    // The name is free again and can be reserved.
    auto reservation = map.reserve("a");
    ASSERT_TRUE(reservation.isValid());
}

TEST_F(ObjectMapTest, publishedNameCannotBeReserved) {
    ObjectMap<TestObject> map;

    {
        auto reservation = map.reserve("a");
        reservation.publish(makeObject(7));
    }

    // The slot is published, so the name stays occupied even after the
    // reservation handle is destroyed.
    auto reservation = map.reserve("a");
    ASSERT_FALSE(reservation.isValid());

    const auto* slot = map.getObject("a");
    ASSERT_NE(slot, nullptr);
    ASSERT_EQ(slot->getObject()->_value, 7);
}

TEST_F(ObjectMapTest, multipleDistinctObjects) {
    ObjectMap<TestObject> map;

    auto first = map.reserve("a");
    auto second = map.reserve("b");
    ASSERT_TRUE(first.isValid());
    ASSERT_TRUE(second.isValid());

    first.publish(makeObject(1));
    second.publish(makeObject(2));

    ASSERT_EQ(map.getObject("a")->getObject()->_value, 1);
    ASSERT_EQ(map.getObject("b")->getObject()->_value, 2);
}

TEST_F(ObjectMapTest, destructionFreesPublishedObjects) {
    {
        ObjectMap<TestObject> map;

        auto first = map.reserve("a");
        auto second = map.reserve("b");
        first.publish(makeObject(1));
        second.publish(makeObject(2));

        ASSERT_EQ(TestObject::_liveCount, 2);
    }

    // The map owns the published objects and frees them on destruction.
    ASSERT_EQ(TestObject::_liveCount, 0);
}

TEST_F(ObjectMapTest, cancelledReservationDoesNotLeak) {
    {
        ObjectMap<TestObject> map;

        auto reservation = map.reserve("a");
        // Never published: nothing was constructed.
        ASSERT_EQ(TestObject::_liveCount, 0);
    }

    ASSERT_EQ(TestObject::_liveCount, 0);
}

TEST_F(ObjectMapTest, concurrentReserveDistinctNames) {
    ObjectMap<TestObject> map;

    constexpr int threadCount = 16;
    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (int i = 0; i < threadCount; ++i) {
        threads.emplace_back([&map, i]() {
            const std::string name = "obj_" + std::to_string(i);

            auto reservation = map.reserve(name);
            ASSERT_TRUE(reservation.isValid());

            reservation.publish(makeObject(i));
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (int i = 0; i < threadCount; ++i) {
        const std::string name = "obj_" + std::to_string(i);

        const auto* slot = map.getObject(name);
        ASSERT_NE(slot, nullptr);
        ASSERT_EQ(slot->getObject()->_value, i);
    }

    ASSERT_EQ(TestObject::_liveCount, threadCount);
}

TEST_F(ObjectMapTest, concurrentReserveSameNameSingleWinner) {
    ObjectMap<TestObject> map;

    constexpr int threadCount = 16;
    std::atomic<int> validCount {0};
    std::atomic<int> arrived {0};
    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (int i = 0; i < threadCount; ++i) {
        threads.emplace_back([&]() {
            auto reservation = map.reserve("contended");
            if (reservation.isValid()) {
                validCount.fetch_add(1, std::memory_order_relaxed);
            }

            // Hold every reservation alive until all threads have tried,
            // so that no cancellation frees the name mid-test and lets a
            // second thread win.
            arrived.fetch_add(1, std::memory_order_acq_rel);
            while (arrived.load(std::memory_order_acquire) < threadCount) {
                std::this_thread::yield();
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(validCount.load(), 1);
}

TEST_F(ObjectMapTest, sizeCountsOnlyPublishedSlots) {
    ObjectMap<TestObject> map;

    ASSERT_EQ(map.size(), 0u);

    auto first = map.reserve("a");

    // A reserved-but-unpublished slot does not count towards the size.
    ASSERT_EQ(map.size(), 0u);

    first.publish(makeObject(1));
    ASSERT_EQ(map.size(), 1u);

    auto second = map.reserve("b");
    second.publish(makeObject(2));
    ASSERT_EQ(map.size(), 2u);
}

TEST_F(ObjectMapTest, cancelledReservationDoesNotChangeSize) {
    ObjectMap<TestObject> map;

    auto published = map.reserve("a");
    published.publish(makeObject(1));
    ASSERT_EQ(map.size(), 1u);

    {
        auto cancelled = map.reserve("b");
        // Reserved but never published.
        ASSERT_EQ(map.size(), 1u);
    }

    // Cancelling the reservation must not touch the published count.
    ASSERT_EQ(map.size(), 1u);
}

TEST_F(ObjectMapTest, listNamesReturnsOnlyPublishedNames) {
    ObjectMap<TestObject> map;

    auto first = map.reserve("a");
    first.publish(makeObject(1));

    auto second = map.reserve("b");
    second.publish(makeObject(2));

    // Reserved but not published: must be excluded from the listing.
    auto pending = map.reserve("c");

    std::vector<std::string_view> names;
    map.listNames(names);

    std::sort(names.begin(), names.end());
    ASSERT_EQ(names.size(), 2u);
    ASSERT_EQ(names[0], "a");
    ASSERT_EQ(names[1], "b");
}

TEST_F(ObjectMapTest, publishedPointersSurviveRehash) {
    ObjectMap<TestObject> map;

    // Publish one object and capture the pointers handed out before the
    // map grows.
    auto first = map.reserve("first");
    first.publish(makeObject(123));

    const auto* slotBefore = map.getObject("first");
    ASSERT_NE(slotBefore, nullptr);
    const TestObject* objectBefore = slotBefore->getObject();

    // Insert many more entries to force the underlying map to rehash.
    constexpr int extraCount = 1024;
    for (int i = 0; i < extraCount; ++i) {
        auto reservation = map.reserve("extra_" + std::to_string(i));
        reservation.publish(makeObject(i));
    }

    // Slots live behind unique_ptr and are never moved, so the slot and the
    // object it owns must still be reachable at the same addresses.
    const auto* slotAfter = map.getObject("first");
    ASSERT_EQ(slotAfter, slotBefore);
    ASSERT_EQ(slotAfter->getObject(), objectBefore);
    ASSERT_EQ(slotAfter->getObject()->_value, 123);
}

TEST_F(ObjectMapTest, concurrentCancelDoesNotRaceReaders) {
    ObjectMap<TestObject> map;

    // A stable published entry that readers can always look up.
    auto stable = map.reserve("stable");
    stable.publish(makeObject(99));

    constexpr int threadCount = 8;
    constexpr int iterations = 200;
    std::vector<std::thread> threads;
    threads.reserve(threadCount * 2);

    // Half the threads repeatedly reserve and cancel distinct names,
    // exercising the unpublished-reservation cleanup path (removeSlot),
    // which erases from the map under the write lock.
    for (int t = 0; t < threadCount; ++t) {
        threads.emplace_back([&map, t]() {
            for (int i = 0; i < iterations; ++i) {
                const std::string name = "tmp_" + std::to_string(t) + "_" + std::to_string(i);

                auto reservation = map.reserve(name);
                ASSERT_TRUE(reservation.isValid());

                // The reservation is destroyed here without publishing,
                // racing the reader threads below.
            }
        });
    }

    // The other half hammer the read paths concurrently.
    for (int t = 0; t < threadCount; ++t) {
        threads.emplace_back([&map]() {
            for (int i = 0; i < iterations; ++i) {
                const auto* slot = map.getObject("stable");
                ASSERT_NE(slot, nullptr);
                ASSERT_EQ(slot->getObject()->_value, 99);

                std::vector<std::string_view> names;
                map.listNames(names);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Only the pre-published entry survives; every temporary was cancelled.
    ASSERT_EQ(map.size(), 1u);

    std::vector<std::string_view> names;
    map.listNames(names);
    ASSERT_EQ(names.size(), 1u);
    ASSERT_EQ(names[0], "stable");
}
