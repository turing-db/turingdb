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

class ColumnDispatcherResizeTest : public ::testing::Test {
protected:
    std::unique_ptr<Column> _column;

    void SetUp() override {
        _column = std::make_unique<ColumnVector<NodeID>>();
    }
};

TEST_F(ColumnDispatcherResizeTest, colVectorResize) {
    constexpr size_t SIZE = 10;

    ASSERT_EQ(_column->size(), 0);

    dispatchColumnVector(_column.get(), [&](auto* c) { c->resize(SIZE); });

    ASSERT_EQ(_column->size(), SIZE);
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
        auto* castedA = static_cast<decltype(col)>(colA.get());
        col->appendRows(*castedA, 0, castedA->size());
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

