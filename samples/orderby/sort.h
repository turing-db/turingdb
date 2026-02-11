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

using namespace db;

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

void sortColRg(Column* col, size_t start, size_t end, std::vector<size_t>& indices) {
    auto* ccol = dynamic_cast<ColumnInts*>(col);
    bioassert(ccol, "Failed to cast column to sort.");
    bioassert(end < ccol->size(), "End out of range.");

    std::vector<Int>& data = ccol->getRaw();

    auto dataRange = rg::subrange(begin(data) + start, begin(data) + end + 1);
    auto indicesRange = rg::subrange(begin(indices) + start, begin(indices) + end + 1);

    auto&& zipped = rv::zip(indicesRange, dataRange);
    rg::sort(zipped, [](auto&& zip1, auto&& zip2) {
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

template <std::ranges::forward_range ColRg, std::ranges::forward_range IndxRg>
void project(const ColRg& cols, IndxRg& indices) {
    std::vector<Int> temp;
    for (NamedColumn* ncol : cols) {
        Column* col = ncol->getColumn();
        auto* ccol = dynamic_cast<ColumnInts*>(col);

        std::vector<Int>& data = ccol->getRaw();
        temp = data;

        for (size_t i {0}; i < indices.size(); i++) {
            assert(idx < data.size());
            assert(idx < indices.size());
            data[i] = temp[indices[i]];
        }
    }
    std::ranges::iota(indices, 0);
}

void sort(Dataframe* df) {
    if (df->getRowCount() <= 1) {
        return;
    }

    const size_t numCols = df->size();
    const size_t numRows = df->getRowCount();
    assert(numCols == 2);

    std::vector<size_t> indices(numRows);
    std::iota(begin(indices), end(indices), 0);

    auto& cols = df->cols();

    Column* dominantCol = cols.front()->getColumn();
    sortCol(dominantCol, indices);

    {
        auto colSubrange = rg::subrange(begin(cols) + 1, end(cols));
        project(colSubrange, indices);
    }
    std::vector<TieRange> tieRanges;
    for (size_t i {1}; i < numCols; i++) {

        auto* prevCol = cols[i - 1]->as<ColumnInts>();
        auto* thisCol = cols[i]->as<ColumnInts>();

        populateTieRanges(tieRanges, prevCol);
        if (tieRanges.empty()) {
            continue;
        }

        std::vector<Int>& data = thisCol->getRaw();

        for (const auto& [start, size] : tieRanges) {
            auto colSubrange = rg::subrange(begin(data) + start, begin(data) + start + size);
            auto idxSubrange = rg::subrange(begin(indices) + start, begin(indices) + start + size);

            rg::sort(rv::zip(idxSubrange, colSubrange), [](auto&& zip1, auto&& zip2) {
                const Int a = std::get<1>(zip1);
                const Int b = std::get<1>(zip2);
                return a < b;
            });
        }
        auto remainingCols = rg::subrange(begin(cols) + i + 1, end(cols));
        project(remainingCols, indices);
    }
}
