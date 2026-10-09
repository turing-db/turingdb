#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <fstream>
#include <span>
#include <string>
#include <string_view>

#if defined(__APPLE__)
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif

#include "NLOutputSink.h"

#include "CallV3Test.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t MEGABYTE = 1024 * 1024;

size_t getHeapBytesInUse() {
#if defined(__APPLE__)
    malloc_statistics_t statistics;
    malloc_zone_statistics(nullptr, &statistics);
    return statistics.size_in_use;
#else
    const struct mallinfo2 info = mallinfo2();
    return info.uordblks + info.hblkhd;
#endif
}

// Samples the heap at every chunk the query emits, so the growth it records is the
// memory the query holds while it runs, not what it leaves behind
class HeapSamplingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        const size_t bytes = getHeapBytesInUse();

        if (_sampleCount == 0) {
            _firstBytes = bytes;
        }

        _peakBytes = std::max(_peakBytes, bytes);
        _rowCount += rowCount;
        _sampleCount++;
    }

    size_t getGrowth() const { return _peakBytes - _firstBytes; }
    size_t getRowCount() const { return _rowCount; }

private:
    size_t _firstBytes {0};
    size_t _peakBytes {0};
    size_t _rowCount {0};
    size_t _sampleCount {0};
};

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
