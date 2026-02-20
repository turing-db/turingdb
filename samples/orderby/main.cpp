#include <limits>
#include <memory>
#include <ranges>
#include <iostream>

#include <spdlog/spdlog.h>

#include "merge.h"
#include "sort.h"
#include "utils.h"
#include "bm.h"

#include "LocalMemory.h"
#include "iterators/ChunkConfig.h"

#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

#include "FatalException.h"

using namespace db;

template<std::ranges::forward_range Rg>
void print_range(Rg&& range, std::string_view name) {
    std::cout << name << ": ";
    for (auto&& x : range) {
        std::cout << x << ' ';
    } std::cout << '\n';
}

void bm(LocalMemory& mem, DataframeManager& dfman) {
    Int minV = std::numeric_limits<Int>::min();
    Int maxV = std::numeric_limits<Int>::max();
    /* benchmarkSort(mem, dfman, ChunkConfig::CHUNK_SIZE, 1, 5, minV, maxV);
    benchmarkSort(mem, dfman, ChunkConfig::CHUNK_SIZE, 2, 5, minV, maxV);
    benchmarkSort(mem, dfman, ChunkConfig::CHUNK_SIZE, 3, 5, minV, maxV);
    benchmarkSort(mem, dfman, ChunkConfig::CHUNK_SIZE, 4, 5, minV, maxV);
    benchmarkSort(mem, dfman, ChunkConfig::CHUNK_SIZE, 5, 5, minV, maxV);
    benchmarkSort(mem, dfman, ChunkConfig::CHUNK_SIZE, 10, 5, minV, maxV);
    benchmarkSort(mem, dfman, ChunkConfig::CHUNK_SIZE, 25, 5, minV, maxV); */


    compareSorts(mem, dfman, ChunkConfig::CHUNK_SIZE, 1, 5, minV, maxV);
    compareSorts(mem, dfman, ChunkConfig::CHUNK_SIZE, 2, 5, minV, maxV);
    compareSorts(mem, dfman, ChunkConfig::CHUNK_SIZE, 3, 5, minV, maxV);
    compareSorts(mem, dfman, ChunkConfig::CHUNK_SIZE, 4, 5, minV, maxV);
    compareSorts(mem, dfman, ChunkConfig::CHUNK_SIZE, 5, 5, minV, maxV);
    compareSorts(mem, dfman, ChunkConfig::CHUNK_SIZE, 10, 5, minV, maxV);
    compareSorts(mem, dfman, ChunkConfig::CHUNK_SIZE, 25, 5, minV, maxV);
}

void test(LocalMemory& mem, DataframeManager& dfman) {
    auto original =
        makeDataframe<Int>(mem, dfman,
                           {
                               {60, 58, 84, 82, 62, 15, 38, 15, 44, 23, 12, 58, 4,
                                37, 44, 60, 0,  92, 30,  17, 83, 2,  25, 27, 11},
                               {20, 51, 25, 69, 18, 38, 19, 69, 30, 64, 44, 38, 62,
                                22, 30, 54, 62, 32, 80,  78, 6,  95, 41, 18, 9 },
                               {4,  19, 17, 82, 35, 25, 24, 44, 3,  8,  97, 86, 17,
                                82, 92, 32, 93, 31, 27,  77, 73, 55, 39, 97, 19},
                               {72, 96, 21, 13, 94, 18, 6,  79, 42, 30, 76, 63, 74,
                                30, 73, 98, 7,  94, 65,  22, 11, 63, 85, 57, 5 },
                               {66, 97, 45, 84, 30, 66, 87, 83, 17, 19, 59, 32, 57,
                                38, 74, 83, 17, 15, 69,  40, 19, 33, 52, 47, 68},
                               {54, 52, 83, 1,  15, 38, 50, 50, 54, 75, 76, 82, 21,
                                46, 34, 6,  60, 35, 45,  87, 48, 53, 3,  48, 79},
                               {10, 71, 34, 11, 67, 13, 96, 99, 83, 82, 32, 84, 29,
                                32, 95, 35, 16, 59, 19,  85, 83, 65, 21, 38, 55},
                               {89, 62, 33, 63, 11, 65, 94, 86, 35, 15, 61, 92, 19,
                                24, 85, 35, 26, 51, 100, 24, 57, 81, 75, 43, 38},
                               {87, 79, 32, 9,  49, 49, 20, 94, 7,  81, 54, 28, 11,
                                94, 84, 13, 67, 47, 80,  40, 21, 21, 2,  49, 83},
                               {23, 37, 5,  21, 24, 34, 78, 54, 63, 51, 9,  90, 70,
                                21, 43, 56, 11, 48, 0,   39, 35, 61, 57, 95, 8 }
    });

    /* auto original =
        makeDataframe<Int>(mem, dfman,
                           {
                               {2, 4, 1, 4, 6},
                               {1, 9, 3, 4, 2},
                               {8, 6, 9, 7, 5}
    }); */

    auto sorted = copyDataframe(mem, dfman, original);
    subsort(sorted.get());
    
    if (!containSame(original, sorted)) {
        original->dump(std::cout);
        sorted->dump(std::cout);
        rowsort(original.get());
        original->dump(std::cout);
        throw FatalException("Not same.");
    }
    if (!isSorted(sorted)) {
        original->dump(std::cout);
        sorted->dump(std::cout);
        throw FatalException("Not sorted.");
    }
    spdlog::info("Tests pass");
}

void mergeTest(LocalMemory& mem, DataframeManager& dfman) {
    auto original = makeDataframe<Int>(mem, dfman,
                                       {
                                           {4, 5, 6, 1, 2, 3}
    });

    const size_t size = original->cols().front()->getColumn()->size();
    std::vector<size_t> indices(size);
    std::ranges::iota(indices, 0);

    const SortedRun run1{._start = 0, ._size = 3};
    const SortedRun run2{._start = 3, ._size = 3};

    merge(indices, original->cols(), run1, run2);

    for (size_t x : indices) {
        std::cout << x << ' ';
    } std::cout << '\n';
}

int main() {
    LocalMemory mem;
    DataframeManager dfman;

    // test(mem, dfman);
    // bm(mem, dfman);
    try {
        mergeTest(mem, dfman);
    } catch (FatalException& e) {
        spdlog::error(e.what());
    }
}
