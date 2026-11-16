#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include "columns/ColumnOptVector.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;

namespace {

constexpr size_t ROW_COUNT = 65536;
constexpr size_t MAX_GROWTH_COUNT = 40;

struct GrowthCounter {
    size_t valueCapacity {0};
    size_t maskCapacity {0};
    size_t valueGrowthCount {0};
    size_t maskGrowthCount {0};

    void observe(const ColumnOptVector<int64_t>& column) {
        const size_t currentValueCapacity = column.getValues().capacity();
        if (currentValueCapacity != valueCapacity) {
            valueCapacity = currentValueCapacity;
            valueGrowthCount++;
        }

        const DynamicLargeBitMask<uint64_t>* validity = column.getValidity();
        const size_t currentMaskCapacity = validity ? validity->capacity() : 0;
        if (currentMaskCapacity != maskCapacity) {
            maskCapacity = currentMaskCapacity;
            maskGrowthCount++;
        }
    }
};

}

TEST(ColumnOptVectorAppendGrowthTest, appendingSingleRowsGrowsGeometrically) {
    ColumnOptVector<int64_t> source;
    source.push_back(7);
    source.pushNull();

    ColumnOptVector<int64_t> column;
    column.pushNull();

    GrowthCounter counter;
    for (size_t row = 1; row < ROW_COUNT; row++) {
        column.appendRows(source, row % 2, 1);
        counter.observe(column);
    }

    EXPECT_LE(counter.valueGrowthCount, MAX_GROWTH_COUNT);
    EXPECT_LE(counter.maskGrowthCount, MAX_GROWTH_COUNT);
    EXPECT_EQ(column.size(), ROW_COUNT);
    EXPECT_EQ(column.getNullCount(), ROW_COUNT / 2 + 1);
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorAppendGrowthTest, appendingValidRowsAfterANullGrowsGeometrically) {
    ColumnOptVector<int64_t> source;
    source.push_back(7);

    ColumnOptVector<int64_t> column;
    column.pushNull();

    GrowthCounter counter;
    for (size_t row = 1; row < ROW_COUNT; row++) {
        column.appendRows(source, 0, 1);
        counter.observe(column);
    }

    EXPECT_LE(counter.valueGrowthCount, MAX_GROWTH_COUNT);
    EXPECT_LE(counter.maskGrowthCount, MAX_GROWTH_COUNT);
    EXPECT_EQ(column.getNullCount(), 1u);
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorAppendGrowthTest, pushingRowsWithAMaskGrowsGeometrically) {
    ColumnOptVector<int64_t> column;
    column.pushNull();

    GrowthCounter counter;
    for (size_t row = 1; row < ROW_COUNT; row++) {
        if (row % 3 == 0) {
            column.pushNull();
        } else {
            column.push_back(static_cast<int64_t>(row));
        }

        counter.observe(column);
    }

    EXPECT_LE(counter.valueGrowthCount, MAX_GROWTH_COUNT);
    EXPECT_LE(counter.maskGrowthCount, MAX_GROWTH_COUNT);
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorAppendGrowthTest, reservedValuesCarryOverToTheMask) {
    ColumnOptVector<int64_t> source;
    source.pushNull();
    source.push_back(7);

    ColumnOptVector<int64_t> column;
    column.reserve(ROW_COUNT);

    GrowthCounter counter;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        column.appendRows(source, row % 2, 1);
        counter.observe(column);
    }

    EXPECT_EQ(counter.valueGrowthCount, 1u);
    EXPECT_EQ(counter.maskGrowthCount, 1u);
    expectColumnOptVectorInvariants(column);
}
