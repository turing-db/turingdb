#pragma once

#include <algorithm>
#include <iostream>
#include <numeric>
#include <ranges>

#include <range/v3/action/sort.hpp>
#include <range/v3/action/stable_sort.hpp>
#include <range/v3/view/enumerate.hpp>
#include "range/v3/view/subrange.hpp"
#include "spdlog/spdlog.h"

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

    rg::stable_sort(rv::zip(indices, *ccol), [](auto&& zip1, auto&& zip2) {
        const Int a = std::get<1>(zip1);
        const Int b = std::get<1>(zip2);
        return a < b;
    });
}

template <std::ranges::random_access_range Rg>
void addTieRanges(std::vector<TieRange>& tieRanges, const Rg& rg, size_t start = 0) {
    // Find the first instance of a duplciated entry in the column
    auto startIt = std::ranges::adjacent_find(rg);

    while (startIt != end(rg)) {
        // Find the interval [start, end) of duplicated entries in column
        auto endIt = startIt;
        while (endIt != end(rg) && *endIt == *startIt) {
            ++endIt;
        }
        const size_t startIdx = std::distance(begin(rg), startIt) + start;
        const size_t size = std::distance(startIt, endIt);
        tieRanges.emplace_back(startIdx, size);
        startIt = std::adjacent_find(endIt, end(rg));
    }
}

void narrowTieRanges(std::vector<TieRange>& tieRanges, Column* col) {
    auto ccol = dynamic_cast<ColumnInts*>(col);
    bioassert(ccol, "Failed to cast column to sort.");

    // Temporary vector which will contain the new tie-ranges
    std::vector<TieRange> temp;
    auto& data = ccol->getRaw();

    for (const auto& [start, size] : tieRanges) {
        const size_t end = start + size;
        auto subrange = rg::subrange(begin(data) + start, begin(data) + end);
        addTieRanges(temp, subrange, start);
    }

    tieRanges.swap(temp);
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

void rowsort(Dataframe* df) {
    // Empty/singleton dataframe is trivially sorted
    if (df->getRowCount() <= 1) {
        return;
    }

    const size_t numRows = df->getRowCount();

    std::vector<size_t> idx(numRows);
    std::iota(idx.begin(), idx.end(), 0);

    rg::sort(idx.begin(), idx.end(), [&](size_t i, size_t j) {
        for (auto* ncol : df->cols()) {
            auto* c = ncol->as<ColumnInts>();
            Int a = (*c)[i];
            Int b = (*c)[j];
            if (a < b) {
                return true;
            }
            if (a > b) {
                return false;
            }
        }
        return false;
    });

    // Materialise permutation
    for (auto* ncol : df->cols()) {
        auto* c = ncol->as<ColumnInts>();
        auto& raw = c->getRaw();
        std::vector<Int> tmp(raw.size());
        for (size_t r = 0; r < numRows; ++r) {
            tmp[r] = raw[idx[r]];
        }
        raw.swap(tmp);
    }
}

void colsort(Dataframe* df) {
    // Empty/singleton dataframe is trivially sorted
    if (df->getRowCount() <= 1) {
        return;
    }

    const size_t numRows = df->getRowCount();

    std::vector<size_t> indices(numRows);
    std::iota(indices.begin(), indices.end(), 0);

    const auto& cols = df->cols();

    // Sort by least dominant key -> most dominant key, stably.
    // This ensures that the order is preserved
    for (auto* nc : std::ranges::reverse_view(cols)) {
        auto* c = nc->getColumn()->cast<ColumnInts>();
        auto& data = c->getRaw();

        rg::stable_sort(indices, [&](size_t i, size_t j) { return data[i] < data[j]; });
    }

    for (auto* ncol : cols) {
        auto* c = ncol->as<ColumnInts>();
        std::vector<Int>& raw = c->getRaw();
        std::vector<Int> tmp(raw.size());
        for (size_t r = 0; r < numRows; ++r) {
            tmp[r] = raw[indices[r]];
        }
        raw.swap(tmp);
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

    // Find runs of contiguous identical values in the previous column(s); only sort
    // those subruns
    std::vector<TieRange> tieRanges;
    {
        std::vector<Int>& data = dominantCol->cast<ColumnInts>()->getRaw();
        addTieRanges(tieRanges, data);
    }

    // Sort w.r.t the remaining order keys
    for (size_t i {1}; i < numCols; i++) {
        auto* thisCol = cols[i]->as<ColumnInts>();
        // Project the new order of indices - determined by the sort of the previous
        // column - onto this column
        project(thisCol, indices);

        // No ties: nothing to sort in this column
        if (tieRanges.empty()) {
            continue;
        }

        std::vector<Int>& data = thisCol->getRaw();

        // For each tie run, r, sort that run, keeping track of the new indices
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

        // Narrow the ties: for a contiguous range [l, r] in the previous column,
        // constrict the range to [l', r'] such that l <= l' and r' <= r and a_i = x for
        // l' <= i <= r', for some x, and for a_i \in @ref thisCol
        narrowTieRanges(tieRanges, thisCol);
    }
}

}
