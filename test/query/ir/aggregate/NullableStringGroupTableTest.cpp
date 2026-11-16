#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLGroupHashTable.h"

#include "columns/ColumnOptVector.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using StringColumn = ColumnOptVector<std::string_view>;
using Table = NLStringGroupHashTable<std::optional<std::string_view>>;

constexpr size_t ROW_COUNT = 130;
constexpr std::array<std::string_view, 3> KEYS {"a", "b", "c"};

std::set<size_t> allRows() {
    std::set<size_t> rows;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        rows.insert(row);
    }

    return rows;
}

void fillColumn(StringColumn& column, size_t shift, const std::set<size_t>& nullRows) {
    column.clear();
    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (nullRows.contains(row)) {
            column.pushNull();
        } else {
            column.push_back(KEYS[(row + shift) % KEYS.size()]);
        }
    }
}

// The groups of the rows of each step, numbered in the order their keys first appear across
// the steps, with the creating rows of each step
struct ExpectedGroups {
    std::map<std::optional<std::string>, size_t> _groupOfKey;

    void assign(const StringColumn& column, std::vector<size_t>& groups, std::vector<size_t>& newGroupRows) {
        for (size_t row = 0; row < column.size(); row++) {
            std::optional<std::string> key;
            if (column.isValid(row)) {
                key = std::string(column.getValue(row));
            }

            const auto [found, created] = _groupOfKey.try_emplace(key, _groupOfKey.size());
            if (created) {
                newGroupRows.push_back(row);
            }

            groups.push_back(found->second);
        }
    }
};

const std::set<size_t> NO_NULLS {};
const std::set<size_t> NULLS_ACROSS_WORD {0, 3, 62, 63, 64, 65, 100, 129};

}

class NullableStringGroupTableTest : public TuringTest {
protected:
    // Assign one step per shape through one table, each shifting which key a row holds
    void expectSteps(const std::vector<std::set<size_t>>& shapes) {
        Table table;
        ExpectedGroups reference;
        size_t groupCount = 0;

        for (size_t step = 0; step < shapes.size(); step++) {
            SCOPED_TRACE(step);

            StringColumn column;
            fillColumn(column, step, shapes[step]);

            std::vector<size_t> groups(ROW_COUNT, NLGroupAssignment::NO_GROUP);
            std::vector<size_t> newGroupRows;
            NLGroupAssignment assignment(groups, newGroupRows, groupCount);

            const Column* keyColumn = &column;
            table.assign(std::span<const Column* const> {&keyColumn, 1}, assignment);

            std::vector<size_t> expectedGroups;
            std::vector<size_t> expectedNewGroupRows;
            reference.assign(column, expectedGroups, expectedNewGroupRows);

            EXPECT_EQ(groups, expectedGroups);
            EXPECT_EQ(newGroupRows, expectedNewGroupRows);
            EXPECT_EQ(groupCount, reference._groupOfKey.size());
        }
    }
};

TEST_F(NullableStringGroupTableTest, withoutNulls) {
    expectSteps({NO_NULLS, NO_NULLS});
}

TEST_F(NullableStringGroupTableTest, withNullsAcrossWords) {
    expectSteps({NULLS_ACROSS_WORD, NO_NULLS, NULLS_ACROSS_WORD});
}

TEST_F(NullableStringGroupTableTest, allNull) {
    expectSteps({allRows(), NULLS_ACROSS_WORD, allRows()});
}

TEST_F(NullableStringGroupTableTest, nullAfterTheLastPresentRun) {
    std::set<size_t> nullRows;
    for (size_t row = 66; row < ROW_COUNT; row++) {
        nullRows.insert(row);
    }

    expectSteps({nullRows});
}
