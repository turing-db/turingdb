#include <algorithm>
#include <numeric>
#include <ranges>

#include <range/v3/action/sort.hpp>
#include <range/v3/view/enumerate.hpp>

#include "columns/Column.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"

#include "BioAssert.h"

namespace db {

namespace rg = ranges;
namespace rv = rg::views;

using Int = types::Int64::Primitive;
using ColumnInts = ColumnVector<Int>;

struct TieRange {
    const size_t start {0};
    const size_t size {0};
};

void sortCol(Column* col, std::vector<size_t>& indices) {
    auto* ccol = dynamic_cast<ColumnInts*>(col);
    bioassert(ccol, "Failed to cast column to sort.");

    rg::sort(rv::zip(indices, *ccol), [](auto&& zip1, auto&& zip2) {
        const Int a = std::get<1>(zip1);
        const Int b = std::get<1>(zip2);
        return a < b;
    });
}

void populateTieRanges(std::vector<TieRange>& tieRanges, Column* col) {
    auto* ccol = dynamic_cast<ColumnInts*>(col);
    bioassert(ccol, "Failed to cast column to sort.");

    tieRanges.clear();

    std::vector<Int>& data = ccol->getRaw();

    // Find the first instance of a duplciated entry in the column
    auto startIt = std::ranges::adjacent_find(data);

    while (startIt != end(data)) {
        // Find the interval [start, end) of duplicated entries in column
        auto endIt = startIt;
        while (endIt != end(data) && *endIt == *startIt) {
            ++endIt;
        }
        const size_t startIdx = std::distance(begin(data), startIt);
        const size_t size = std::distance(startIt, endIt);
        tieRanges.emplace_back(startIdx, size);
        startIt = std::adjacent_find(endIt, end(data));
    }
}

template <std::ranges::forward_range IndxRg>
void project(Column* col, IndxRg& indices) {
    std::vector<Int> temp;
    auto* ccol = dynamic_cast<ColumnInts*>(col);
    std::vector<Int>& data = ccol->getRaw();
    temp = data;

    for (size_t i {0}; i < indices.size(); i++) {
        assert(i < data.size());
        assert(i < indices.size());
        data[i] = temp[indices[i]];
    }
}

void subsort(Dataframe* df) {
    // Empty/singleton dataframe is trivially sorted
    if (df->getRowCount() <= 1) {
        return;
    }

    const size_t numCols = df->size();
    const size_t numRows = df->getRowCount();

    // Whenever sorting, track the row indexes to materialise post-sort
    std::vector<size_t> indices(numRows);
    std::iota(begin(indices), end(indices), 0);

    const auto& cols = df->cols();

    // Sort w.r.t the most dominant order key
    Column* dominantCol = cols.front()->getColumn();
    sortCol(dominantCol, indices);

    std::vector<TieRange> tieRanges;

    // Sort w.r.t the remaining order keys
    for (size_t i {1}; i < numCols; i++) {
        auto* prevCol = cols[i - 1]->as<ColumnInts>();
        auto* thisCol = cols[i]->as<ColumnInts>();
        project(thisCol, indices);

        // Find runs of contiguous identical values in the previous column; only sort
        // those subruns
        populateTieRanges(tieRanges, prevCol);
        // No ties: nothing to sort in this column
        if (tieRanges.empty()) {
            continue;
        }

        std::vector<Int>& data = thisCol->getRaw();

        // For each tie run, r, sort only that run, keeping track of the new indices
        for (const auto& [start, size] : tieRanges) {
            const size_t end = start + size;
            auto colTieRange = rg::subrange(begin(data) + start, begin(data) + end);
            auto idxSubrange = rg::subrange(begin(indices) + start, begin(indices) + end);

            rg::sort(rv::zip(idxSubrange, colTieRange), [](auto&& zip1, auto&& zip2) {
                const Int a = std::get<1>(zip1);
                const Int b = std::get<1>(zip2);
                return a < b;
            });
        }
    }
}

}
