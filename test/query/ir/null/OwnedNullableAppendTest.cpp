#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "NLExecutor.h"
#include "NLProgram.h"
#include "NLValueStore.h"

#include "columns/ColumnOptVector.h"

#include "ColumnOptVectorInvariants.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using StringColumn = ColumnOptVector<std::string_view>;
using Rows = std::vector<std::optional<std::string>>;

constexpr size_t ROW_COUNT = 130;

std::set<size_t> allRows(size_t rowCount) {
    std::set<size_t> rows;
    for (size_t row = 0; row < rowCount; row++) {
        rows.insert(row);
    }

    return rows;
}

// One chunk of views into text, which the producing step overwrites when it runs again
struct Chunk {
    std::vector<std::string> _text;
    StringColumn _column;
};

void fillChunk(Chunk& chunk, size_t chunkIndex, const std::set<size_t>& nullRows, Rows& expected) {
    chunk._text.assign(ROW_COUNT, std::string {});
    chunk._column.clear();

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (nullRows.contains(row)) {
            chunk._column.pushNull();
            expected.push_back(std::nullopt);
        } else {
            chunk._text[row] = "chunk " + std::to_string(chunkIndex) + " row " + std::to_string(row);
            chunk._column.push_back(chunk._text[row]);
            expected.push_back(chunk._text[row]);
        }
    }
}

void expectRows(const StringColumn& column, const Rows& expected) {
    expectColumnOptVectorInvariants(column);
    ASSERT_EQ(column.size(), expected.size());

    size_t nullCount = 0;
    for (size_t row = 0; row < expected.size(); row++) {
        const std::optional<std::string_view> value = column.get(row);
        if (expected[row].has_value()) {
            ASSERT_TRUE(value.has_value()) << "row " << row;
            EXPECT_EQ(*value, *expected[row]) << "row " << row;
        } else {
            EXPECT_FALSE(value.has_value()) << "row " << row;
            nullCount++;
        }
    }

    EXPECT_EQ(column.getNullCount(), nullCount);
    EXPECT_EQ(column.hasMask(), nullCount != 0);
}

const std::set<size_t> NO_NULLS {};
const std::set<size_t> NULLS_ACROSS_WORD {0, 3, 62, 63, 64, 65, 100, 129};

}

class OwnedNullableAppendTest : public TuringTest {
protected:
    // Append one chunk per shape to one buffer, rewriting the chunk's text after each append
    void expectAppends(const std::vector<std::set<size_t>>& shapes) {
        StringColumn buffer;
        const NLOwnedAppendFunction append = NLExecutor::selectOwnedAppendFunction(&buffer);
        ASSERT_NE(append, nullptr);

        NLValueStore values;
        Chunk chunk;
        Rows expected;

        for (size_t chunkIndex = 0; chunkIndex < shapes.size(); chunkIndex++) {
            fillChunk(chunk, chunkIndex, shapes[chunkIndex], expected);
            append(&chunk._column, &buffer, values);

            for (std::string& text : chunk._text) {
                text.assign(text.size(), '#');
            }

            ASSERT_NO_FATAL_FAILURE(expectRows(buffer, expected));
        }
    }
};

TEST_F(OwnedNullableAppendTest, withoutNulls) {
    expectAppends({NO_NULLS, NO_NULLS});
}

TEST_F(OwnedNullableAppendTest, withNullsAcrossWords) {
    expectAppends({NULLS_ACROSS_WORD, NULLS_ACROSS_WORD});
}

TEST_F(OwnedNullableAppendTest, withoutNullsAfterNulls) {
    expectAppends({NULLS_ACROSS_WORD, NO_NULLS});
}

TEST_F(OwnedNullableAppendTest, withNullsAfterNone) {
    expectAppends({NO_NULLS, NULLS_ACROSS_WORD});
}

TEST_F(OwnedNullableAppendTest, allNull) {
    expectAppends({allRows(ROW_COUNT), NO_NULLS, allRows(ROW_COUNT)});
}
