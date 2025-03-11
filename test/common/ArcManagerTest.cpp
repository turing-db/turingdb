#include "TuringTest.h"

#include "ArcManager.h"

using namespace turing::test;

class ArcManagerTest : public TuringTest {
protected:
    void initialize() override {
    }

    void terminate() override {
    }
};

struct MyObject {
    size_t a {0};
    size_t b {0};
};

TEST_F(ArcManagerTest, empty) {
    ArcManager<MyObject> manager {};
    EXPECT_EQ(manager.size(), 0);

    const size_t deletedCount = manager.cleanUp();
    EXPECT_EQ(deletedCount, 0);
}

TEST_F(ArcManagerTest, deleteAll) {
    ArcManager<MyObject> manager {};

    {
        WeakArc<MyObject> ref1 = manager.create(1, 2);
        WeakArc<MyObject> ref2 = manager.create(2, 3);
        WeakArc<MyObject> ref3 = manager.create(4, 5);

        // Weak references die at the end of the scope
        // The manager will delete the objects on cleanUp()
    }

    EXPECT_EQ(manager.size(), 3);

    const size_t deletedCount = manager.cleanUp();
    EXPECT_EQ(deletedCount, 3);

    EXPECT_EQ(manager.size(), 0);
}

TEST_F(ArcManagerTest, deleteSome) {
    ArcManager<MyObject> manager {};

    WeakArc<MyObject> ref1 = manager.create(1, 2);

    {
        WeakArc<MyObject> ref2 = manager.create(2, 3);
        WeakArc<MyObject> ref3 = manager.create(4, 5);

        // ref2 and ref3 die at the end of the scope
        // The manager will delete them on cleanUp()
    }

    EXPECT_EQ(manager.size(), 3);

    const size_t deletedCount = manager.cleanUp();
    EXPECT_EQ(deletedCount, 2);

    EXPECT_EQ(manager.size(), 1);
}
