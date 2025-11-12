#include <gtest/gtest.h>

#include <memory>

#include "columns/ColumnDispatcher.h"

using namespace db;

TEST(ColumnDispatcherTest, colVectorSize) {
    constexpr size_t SIZE = 10;

    std::unique_ptr<Column> col = std::make_unique<ColumnVector<NodeID>>();

    {
        ColumnVector<NodeID>* casted = dynamic_cast<ColumnVector<NodeID>*>(col.get());
        for (size_t i = 0; i < SIZE; i++) {
            casted->emplace_back(0);
        }
    }

    size_t dispatchedSize =
        dispatchColumnVector(col.get(), [](auto* c) { return c->size(); });

    ASSERT_EQ(SIZE, dispatchedSize);
}

TEST(ColumnDispatcherTest, colVectorResize) {
    constexpr size_t SIZE = 10;

    std::unique_ptr<Column> col = std::make_unique<ColumnVector<NodeID>>();

    ASSERT_EQ(col->size(), 0);

    dispatchColumnVector(col.get(), [&](auto* c) { c->resize(SIZE); });

    ASSERT_EQ(col->size(), SIZE);
}

TEST(ColumnDispatcherTest, copyOneFromOther) {
    constexpr size_t SIZE = 10;

    std::unique_ptr<Column> colA = std::make_unique<ColumnVector<NodeID>>();
    std::unique_ptr<Column> colB = std::make_unique<ColumnVector<NodeID>>();

    {
        ColumnVector<NodeID>* casted = dynamic_cast<ColumnVector<NodeID>*>(colA.get());
        for (size_t i = 0; i < SIZE; i++) {
            casted->emplace_back(NodeID {i});
        }
    }

    ASSERT_EQ(colA->size(), SIZE);
    {
        for (size_t i = 0; i < SIZE; i++) {
            ColumnVector<NodeID>* casted =
                dynamic_cast<ColumnVector<NodeID>*>(colA.get());
            ASSERT_EQ(casted->at(i), i);
        }
    }

    dispatchColumnVector(colB.get(), [&](auto* col) {
        auto* castedA = dynamic_cast<decltype(col)>(colA.get());
        auto& rawB = col->getRaw();
        rawB.insert(col->begin(), castedA->begin(), castedA->end());
    });

    ASSERT_EQ(colA->size(), colB->size());
    {
        for (size_t i = 0; i < SIZE; i++) {
            ColumnVector<NodeID>* castedA =
                dynamic_cast<ColumnVector<NodeID>*>(colA.get());
            ColumnVector<NodeID>* castedB =
                dynamic_cast<ColumnVector<NodeID>*>(colB.get());
            ASSERT_EQ(castedA->at(i), castedB->at(i));
        }
    }
}

