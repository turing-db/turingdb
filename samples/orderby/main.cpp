#include <algorithm>
#include <iostream>
#include <memory>
#include <numeric>

#include <spdlog/spdlog.h>

#include <range/v3/action/sort.hpp>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/transform.hpp>

#include "dataframe/Dataframe.h"
#include "columns/ColumnVector.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"

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
    auto startIt = begin(data) + start;
    auto endIt = begin(data) + end;
    // TODO Sort
    auto&& x = rv::zip(indices, *ccol);
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
        for (; endIt != end(data); endIt++) {
            // Current != next: end this range here
            if (endIt != startIt) {
                const size_t startIdx = std::distance(begin(data), startIt);
                const size_t size = std::distance(startIt, endIt);

                tieRanges.emplace_back(startIdx, size);

                break;
            }
        }
        startIt = std::adjacent_find(endIt, end(data));
    }
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

    Column* dominantCol = df->cols().front()->getColumn();
    sortCol(dominantCol, indices);

    std::vector<TieRange> tieRanges;
    const auto& cols = df->cols();
    for (size_t i {1}; i < numCols; i++) {
        auto* prevcol = cols[i - 1]->as<ColumnInts>();
        auto* thisCol = cols[i]->as<ColumnInts>();

        populateTieRanges(tieRanges, prevcol);

        for (auto& [start, size] : tieRanges) {
            
        }
    }
}

int main() {
    DataframeManager dfman;
    auto df = std::make_unique<Dataframe>();

    std::vector<ColumnInts> cols(2);
    {
        cols[0] = {2, 4, 1, 4, 6};
        cols[1] = {1, 2, 3, 4};

        for (auto&& col : cols) {
            const ColumnTag t = dfman.allocTag();
            NamedColumn* ncol = NamedColumn::create(&dfman, &col, t);
            df->addColumn(ncol);
        }
    }

    spdlog::info("Pre sort:");
    df->dump(std::cout);

    sort(df.get());

    fmt::print("\n\n\n");

    spdlog::info("Post sort:");
    df->dump(std::cout);
}
