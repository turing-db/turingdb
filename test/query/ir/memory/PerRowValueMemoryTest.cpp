#include <gtest/gtest.h>

#include <stddef.h>

#include <fstream>
#include <string>
#include <string_view>

#include "CallV3Test.h"
#include "HeapSamplingSink.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t MEGABYTE = 1024 * 1024;

}

// Every query below builds one value per row over millions of rows. A value only has to
// live until the step that built it comes round again, so the heap stays within a few
// chunks of them.
class PerRowValueMemoryTest : public CallV3Test {
protected:
    void expectBoundedGrowth(std::string_view query, size_t rowCount = 8000000) {
        HeapSamplingSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRowCount(), rowCount) << query;
        EXPECT_LT(sink.getGrowth(), 32 * MEGABYTE) << query << "\ngrew by " << sink.getGrowth() / MEGABYTE << " MB";
    }
};

TEST_F(PerRowValueMemoryTest, buildsARangePerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN range(j, j + 4)");
}

TEST_F(PerRowValueMemoryTest, buildsAListPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN [i, j]");
}

TEST_F(PerRowValueMemoryTest, concatenatesListsPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN [i] + [j]");
}

TEST_F(PerRowValueMemoryTest, buildsAMapPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN {a: i, b: j}");
}

TEST_F(PerRowValueMemoryTest, buildsAStringPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN toString(i * 1000000000 + j)");
}

TEST_F(PerRowValueMemoryTest, concatenatesStringsPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN 'item ' + toString(j)");
}

TEST_F(PerRowValueMemoryTest, replacesInAStringPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN replace(toString(i * 1000000000 + j), '0', 'zero')");
}

TEST_F(PerRowValueMemoryTest, buildsAListComprehensionPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j RETURN [x IN [i, j] | x + 1]");
}

TEST_F(PerRowValueMemoryTest, reducesToAStringPerRow) {
    expectBoundedGrowth("UNWIND range(1, 8000) AS i UNWIND range(1, 1000) AS j "
                        "RETURN reduce(text = '', x IN [i, j] | text + toString(x))");
}

TEST_F(PerRowValueMemoryTest, readsTheFieldsOfACSVFile) {
    constexpr size_t ROW_COUNT = 4000000;

    {
        std::ofstream file(_outDir + "/turing/data/rows.csv");
        file << "name\n";

        for (size_t row = 0; row < ROW_COUNT; row++) {
            file << "item-" << row << "-abcdefghijklmnopqrstuvwxyz\n";
        }
    }

    expectBoundedGrowth("LOAD CSV 'rows.csv' WITH HEADERS AS row RETURN row.name", ROW_COUNT);
}
