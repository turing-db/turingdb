#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <array>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "NLExecutor.h"
#include "NLGroupHashTable.h"
#include "NLProgram.h"
#include "NLSerializedKey.h"
#include "NLValueStore.h"

#include "CellView.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"

#include "ColumnOptVectorInvariants.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using Int64Column = ColumnOptVector<int64_t>;

constexpr size_t SIZES[] = {63, 64, 65, 129};
constexpr uint8_t UNTOUCHED = 0xAB;

int64_t valueOf(size_t row) {
    return static_cast<int64_t>(row % 37) * 1000 - 7;
}

std::set<size_t> wordBoundaryNulls(size_t size) {
    std::set<size_t> rows;
    for (const size_t row : {size_t {0}, size_t {1}, size_t {62}, size_t {63}, size_t {64}, size_t {65}, size - 1}) {
        if (row < size) {
            rows.insert(row);
        }
    }

    return rows;
}

std::set<size_t> allRows(size_t size) {
    std::set<size_t> rows;
    for (size_t row = 0; row < size; row++) {
        rows.insert(row);
    }

    return rows;
}

void nullShapes(size_t size, std::vector<std::set<size_t>>& shapes) {
    shapes = {std::set<size_t> {}, wordBoundaryNulls(size), allRows(size)};
}

void fillColumn(Int64Column& column, size_t size, const std::set<size_t>& nullRows) {
    column.clear();
    for (size_t row = 0; row < size; row++) {
        if (nullRows.contains(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

struct RowRange {
    size_t _begin {0};
    size_t _end {0};
};

constexpr size_t GROUP_COUNT = 3;

void groupsOf(size_t size, std::vector<size_t>& groups, ColumnVector<size_t>& groupColumn) {
    groups.resize(size);
    groupColumn.clear();
    for (size_t row = 0; row < size; row++) {
        groups[row] = row % GROUP_COUNT;
        groupColumn.push_back(groups[row]);
    }
}

// Each present row's list holds that row's value alone, so two rows whose values repeat hold
// equal lists
void fillListColumn(ColumnOptVector<ListView>& column,
                    ListBuffer<>& lists,
                    size_t size,
                    const std::set<size_t>& nullRows) {
    column.clear();
    for (size_t row = 0; row < size; row++) {
        if (nullRows.contains(row)) {
            column.pushNull();
        } else {
            const std::array<ListBuffer<>::ListItemVariant, 1> items {valueOf(row)};
            column.push_back(lists.insert(items));
        }
    }
}

// The values collect keeps from one step: every present row's, or with @param distinct only
// the first of each value in each group, at the position it lands in after @param firstPosition
void expectedCollect(size_t size,
                     const std::set<size_t>& nullRows,
                     const std::vector<size_t>& groups,
                     bool distinct,
                     size_t firstPosition,
                     std::vector<int64_t>& expectedValues,
                     std::vector<std::vector<size_t>>& expectedPositions) {
    expectedPositions.assign(GROUP_COUNT, {});
    std::set<std::pair<size_t, int64_t>> seen;

    for (size_t row = 0; row < size; row++) {
        if (nullRows.contains(row)) {
            continue;
        }

        const bool isNew = seen.insert({groups[row], valueOf(row)}).second;
        if (distinct && !isNew) {
            continue;
        }

        expectedPositions[groups[row]].push_back(firstPosition + expectedValues.size());
        expectedValues.push_back(valueOf(row));
    }
}

void rowRanges(size_t size, std::vector<RowRange>& ranges) {
    ranges = {RowRange {0, size}, RowRange {1, size - 1}, RowRange {62, std::min(size, size_t {66})}, RowRange {size - 1, size}};
}

}

class NullableKeyReaderTest : public TuringTest {
};

// A present row packs its value; a null row sets its null bit and leaves its value bytes as
// they were
TEST_F(NullableKeyReaderTest, packsPresentValuesAndNullBits) {
    const NLKeyPacking packing = NLExecutor::selectOptKeyPacking(NLChunkKind::Int64);
    ASSERT_EQ(packing._width, sizeof(int64_t));

    constexpr size_t VALUE_STRIDE = 16;
    constexpr size_t NULL_BIT = 10;

    for (const size_t size : SIZES) {
        std::vector<std::set<size_t>> shapes;
        nullShapes(size, shapes);

        std::vector<RowRange> ranges;
        rowRanges(size, ranges);

        for (const std::set<size_t>& nullRows : shapes) {
            Int64Column column;
            fillColumn(column, size, nullRows);

            for (const RowRange& range : ranges) {
                const size_t batchRows = range._end - range._begin;
                std::vector<std::byte> keys(batchRows * VALUE_STRIDE, std::byte {UNTOUCHED});

                NLKeyPackTarget target;
                target._values = keys.data() + 4;
                target._valueStride = VALUE_STRIDE;
                target._nulls = keys.data();
                target._nullStride = VALUE_STRIDE;
                target._nullBit = NULL_BIT;

                packing._pack(&column, range._begin, range._end, target);

                for (size_t index = 0; index < batchRows; index++) {
                    const size_t row = range._begin + index;
                    const std::byte* key = keys.data() + index * VALUE_STRIDE;
                    const uint8_t nullByte = static_cast<uint8_t>(key[NULL_BIT / 8]);

                    int64_t packed = 0;
                    memcpy(&packed, key + 4, sizeof(packed));

                    if (nullRows.contains(row)) {
                        EXPECT_EQ(nullByte, UNTOUCHED | (1u << (NULL_BIT % 8))) << "size " << size << " row " << row;

                        std::array<uint8_t, sizeof(int64_t)> valueBytes {};
                        memcpy(valueBytes.data(), key + 4, sizeof(int64_t));
                        for (const uint8_t byte : valueBytes) {
                            EXPECT_EQ(byte, UNTOUCHED) << "size " << size << " row " << row;
                        }
                    } else {
                        EXPECT_EQ(nullByte, UNTOUCHED) << "size " << size << " row " << row;
                        EXPECT_EQ(packed, valueOf(row)) << "size " << size << " row " << row;
                    }
                }
            }
        }
    }
}

// A null row hashes to 0 and a present row as the plain column holding its value does
TEST_F(NullableKeyReaderTest, hashesPresentValuesAndZeroForNulls) {
    const NLJoinKeyFunctions optKeys = NLExecutor::selectOptJoinKeyFunctions(NLChunkKind::Int64);
    const NLJoinKeyFunctions plainKeys = NLExecutor::selectJoinKeyFunctions(NLChunkKind::Int64);

    for (const size_t size : SIZES) {
        std::vector<std::set<size_t>> shapes;
        nullShapes(size, shapes);

        ColumnVector<int64_t> plain;
        for (size_t row = 0; row < size; row++) {
            plain.push_back(valueOf(row));
        }

        std::vector<uint64_t> plainHashes;
        plainKeys._hash(&plain, plainHashes);

        std::vector<uint64_t> hashes {1, 2, 3};
        for (const std::set<size_t>& nullRows : shapes) {
            Int64Column column;
            fillColumn(column, size, nullRows);

            optKeys._hash(&column, hashes);
            ASSERT_EQ(hashes.size(), size);

            for (size_t row = 0; row < size; row++) {
                const uint64_t expected = nullRows.contains(row) ? 0 : plainHashes[row];
                EXPECT_EQ(hashes[row], expected) << "size " << size << " row " << row;
                EXPECT_EQ(optKeys._isMatchable(&column, row), !nullRows.contains(row)) << "size " << size << " row " << row;
            }
        }
    }
}

// A null row keys as the tag byte alone, a present row as the present tag then its bytes
TEST_F(NullableKeyReaderTest, serializesATagThenThePresentValue) {
    const NLKeyAppendFunction append = NLExecutor::selectOptKeyAppendFunction(NLChunkKind::Int64);

    for (const size_t size : SIZES) {
        std::vector<std::set<size_t>> shapes;
        nullShapes(size, shapes);

        for (const std::set<size_t>& nullRows : shapes) {
            Int64Column column;
            fillColumn(column, size, nullRows);

            NLSerializedKey key;
            for (size_t row = 0; row < size; row++) {
                key.clear();
                append(&column, row, key);

                std::string expected;
                if (nullRows.contains(row)) {
                    expected.push_back('\0');
                } else {
                    const int64_t value = valueOf(row);
                    expected.push_back('\1');
                    expected.append(reinterpret_cast<const char*>(&value), sizeof(value));
                }

                EXPECT_EQ(key.getBytes(), expected) << "size " << size << " row " << row;
            }
        }
    }
}

// The gathered rows land on the buffer's tail in the order asked, a row asked twice landing
// twice, over a buffer that already holds rows
TEST_F(NullableKeyReaderTest, gathersGroupKeysOntoAReusedBuffer) {
    const NLGroupKeyGatherFunction gather = NLExecutor::selectOptGroupKeyGather(NLChunkKind::Int64);

    for (const size_t size : SIZES) {
        std::vector<size_t> rows;
        for (size_t row = 0; row < size; row += 2) {
            rows.push_back(row);
        }
        rows.push_back(size - 1);
        rows.push_back(63 % size);
        rows.push_back(0);

        NLValueStore values;
        Int64Column buffer;
        std::vector<std::optional<int64_t>> expected;

        for (const std::set<size_t>& nullRows : {wordBoundaryNulls(size), std::set<size_t> {}, allRows(size), std::set<size_t> {}}) {
            Int64Column input;
            fillColumn(input, size, nullRows);

            gather(&input, rows, &buffer, values);
            for (const size_t row : rows) {
                expected.push_back(input.get(row));
            }

            expectColumnOptVectorInvariants(buffer);
            ASSERT_EQ(buffer.size(), expected.size());
            for (size_t row = 0; row < expected.size(); row++) {
                EXPECT_EQ(buffer.get(row), expected[row]) << "size " << size << " row " << row;
            }
        }
    }
}

// collect appends each present value once, in row order, under its row's group
TEST_F(NullableKeyReaderTest, collectsPresentValuesUnderTheirGroups) {
    const NLCollectFoldFunction fold = NLExecutor::selectCollectFold(NLChunkKind::Int64);

    for (const size_t size : SIZES) {
        std::vector<std::set<size_t>> shapes;
        nullShapes(size, shapes);

        std::vector<size_t> groups(size);
        for (size_t row = 0; row < size; row++) {
            groups[row] = row % 3;
        }

        for (const std::set<size_t>& nullRows : shapes) {
            Int64Column input;
            fillColumn(input, size, nullRows);

            ColumnVector<int64_t> collected;
            collected.push_back(-1);
            std::vector<std::vector<size_t>> groupPositions(3);
            NLValueStore store;

            fold(&collected, &input, groups, groupPositions, nullptr, store);

            std::vector<int64_t> expectedValues {-1};
            std::vector<std::vector<size_t>> expectedPositions(3);
            for (size_t row = 0; row < size; row++) {
                if (!nullRows.contains(row)) {
                    expectedPositions[groups[row]].push_back(expectedValues.size());
                    expectedValues.push_back(valueOf(row));
                }
            }

            EXPECT_EQ(collected.getRaw(), expectedValues) << "size " << size;
            EXPECT_EQ(groupPositions, expectedPositions) << "size " << size;
        }
    }
}

TEST_F(NullableKeyReaderTest, collectsDistinctValuesUnderTheirGroups) {
    const NLCollectFoldFunction fold = NLExecutor::selectCollectDistinctFold(NLChunkKind::Int64);

    NLGroupHashKey valueKey;
    valueKey._nullable = true;
    valueKey._keyAppend = NLExecutor::selectOptKeyAppendFunction(NLChunkKind::Int64);

    for (const size_t size : SIZES) {
        std::vector<std::set<size_t>> shapes;
        nullShapes(size, shapes);

        std::vector<size_t> groups;
        ColumnVector<size_t> groupColumn;
        groupsOf(size, groups, groupColumn);

        for (const std::set<size_t>& nullRows : shapes) {
            Int64Column input;
            fillColumn(input, size, nullRows);

            NLGroupDistinctTally distinct;
            distinct.setValueKey(valueKey);
            distinct.assign(&groupColumn, &input);

            ColumnVector<int64_t> collected;
            collected.push_back(-1);
            std::vector<std::vector<size_t>> groupPositions(GROUP_COUNT);
            NLValueStore store;

            fold(&collected, &input, groups, groupPositions, &distinct, store);

            std::vector<int64_t> expectedValues;
            std::vector<std::vector<size_t>> expectedPositions;
            expectedCollect(size, nullRows, groups, true, 1, expectedValues, expectedPositions);
            expectedValues.insert(expectedValues.begin(), -1);

            EXPECT_EQ(collected.getRaw(), expectedValues) << "size " << size;
            EXPECT_EQ(groupPositions, expectedPositions) << "size " << size;
        }
    }
}

// The list sibling of collectsPresentValuesUnderTheirGroups and of its distinct form: a null
// list is dropped, and a distinct collect keeps the first equal list of each group
TEST_F(NullableKeyReaderTest, collectsPresentListsUnderTheirGroups) {
    NLCollectFoldFunction folds[2] {nullptr, nullptr};
    NLCollectListEmitFunction listEmit = nullptr;
    NLExecutor::selectCollectOptContainerHandlers<ListView>(false, folds[0], listEmit);
    NLExecutor::selectCollectOptContainerHandlers<ListView>(true, folds[1], listEmit);

    NLGroupHashKey valueKey;
    valueKey._nullable = true;
    valueKey._keyAppend = NLExecutor::selectOptListKeyAppendFunction();

    for (const size_t size : SIZES) {
        std::vector<std::set<size_t>> shapes;
        nullShapes(size, shapes);

        std::vector<size_t> groups;
        ColumnVector<size_t> groupColumn;
        groupsOf(size, groups, groupColumn);

        for (const std::set<size_t>& nullRows : shapes) {
            for (const bool isDistinct : {false, true}) {
                ListBuffer<> lists;
                ColumnOptVector<ListView> input;
                fillListColumn(input, lists, size, nullRows);

                NLGroupDistinctTally distinct;
                distinct.setValueKey(valueKey);
                distinct.assign(&groupColumn, &input);

                ColumnVector<ListView> collected;
                std::vector<std::vector<size_t>> groupPositions(GROUP_COUNT);
                NLValueStore store;

                folds[isDistinct ? 1 : 0](&collected, &input, groups, groupPositions, &distinct, store);

                std::vector<int64_t> expectedValues;
                std::vector<std::vector<size_t>> expectedPositions;
                expectedCollect(size, nullRows, groups, isDistinct, 0, expectedValues, expectedPositions);

                std::vector<int64_t> collectedValues;
                for (const ListView list : collected.getRaw()) {
                    ASSERT_EQ(list.size(), 1u);
                    collectedValues.push_back(list.elements()[0].getAs<int64_t>());
                }

                EXPECT_EQ(collectedValues, expectedValues) << "size " << size << " distinct " << isDistinct;
                EXPECT_EQ(groupPositions, expectedPositions) << "size " << size << " distinct " << isDistinct;
            }
        }
    }
}
