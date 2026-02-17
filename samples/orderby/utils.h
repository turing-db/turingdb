#pragma once

#include <iostream>
#include <memory>
#include <chrono>
#include <random>

#include <spdlog/spdlog.h>

#include "FatalException.h"
#include "sort.h"

#include "LocalMemory.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "metadata/PropertyType.h"

namespace db {

using Df = std::unique_ptr<Dataframe>;
using Int = types::Int64::Primitive;
using ColumnInts = ColumnVector<Int>;

// moves each vector into a ColumnVector<T> and adds that as a column
template <typename T>
Df makeDataframe(LocalMemory& mem, DataframeManager& dfman, std::vector<std::vector<T>>&& cols) {
    auto df = std::make_unique<Dataframe>();
    
    for (auto&& colData : cols) {
        auto* columnVec = mem.alloc<ColumnVector<T>>(std::move(colData));
        const ColumnTag tag = dfman.allocTag();
        NamedColumn* ncol = NamedColumn::create(&dfman, columnVec, tag);
        df->addColumn(ncol);
    }
    
    return df;
}

void duplicateDataframeShape(LocalMemory* mem,
                             DataframeManager* dfMan,
                             Dataframe* src,
                             Dataframe* dest) {
    for (const NamedColumn* col : src->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());
        dest->addColumn(newNamedCol);
    }
}

Df copyDataframe(LocalMemory& mem, DataframeManager& dfman, const Df& toCopy) {
    auto newDf = std::make_unique<Dataframe>();
    duplicateDataframeShape(&mem, &dfman, toCopy.get(), newDf.get());
    newDf->copyFrom(toCopy.get());
    return newDf;
}

// check each column in the df to see if its sorted
bool isSorted(const Df& df) {
    const size_t numRows = df->getLogicalRowCount();
    if (numRows <= 1) return true;
    
    const auto& cols = df->cols();
    
    // compare each adjacent pair of rows
    for (size_t row = 0; row < numRows - 1; row++) {
        // Compare row with row+1 across all columns
        for (NamedColumn* ncol : cols) {
            auto* ccol = ncol->as<ColumnInts>();
            Int val1 = (*ccol)[row];
            Int val2 = (*ccol)[row + 1];
            
            if (val1 < val2) {
                // row < row+1, this is correct ordering, check next row
                break;
            } else if (val1 > val2) {
                // row > row+1, this violates sort order
                return false;
            }
            // val1 == val2, continue to next column for tiebreaker
        }
    }
    
    return true;
}

bool containSame(const Df& a, const Df& b) {
    if (a->getLogicalRowCount() != b->getLogicalRowCount()) {
        return false;
    }
    if (a->cols().size() != b->cols().size()) {
        return false;
    }

    auto makeRows = [](const Df& df) {
        const auto& cols = df->cols();
        size_t rows = df->getLogicalRowCount();

        std::vector<std::vector<Int>> out(rows);

        for (auto* ncol : cols) {
            auto* c = ncol->as<ColumnInts>();
            bioassert(c, "Non-int column");

            for (size_t r = 0; r < rows; ++r) {
                out[r].push_back((*c)[r]);
            }
        }

        std::ranges::sort(out);
        return out;
    };

    return makeRows(a) == makeRows(b);
}

Df makeRandomDataframe(LocalMemory& mem, DataframeManager& dfman, 
                       size_t numRows, size_t numCols,
                       Int minVal = 0, Int maxVal = 100) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<Int> dist(minVal, maxVal);
    
    std::vector<std::vector<Int>> cols;
    cols.reserve(numCols);
    
    for (size_t col = 0; col < numCols; col++) {
        std::vector<Int> colData;
        colData.reserve(numRows);
        
        for (size_t row = 0; row < numRows; row++) {
            colData.push_back(dist(gen));
        }
        
        cols.push_back(std::move(colData));
    }
    
    return makeDataframe<Int>(mem, dfman, std::move(cols));
}

// Overload with seed for reproducible tests
Df makeRandomDataframe(LocalMemory& mem, DataframeManager& dfman, 
                       size_t numRows, size_t numCols,
                       unsigned int seed,
                       Int minVal = 0, Int maxVal = 100) {
    std::mt19937 gen(seed);
    std::uniform_int_distribution<Int> dist(minVal, maxVal);
    
    std::vector<std::vector<Int>> cols;
    cols.reserve(numCols);
    
    for (size_t col = 0; col < numCols; col++) {
        std::vector<Int> colData;
        colData.reserve(numRows);
        
        for (size_t row = 0; row < numRows; row++) {
            colData.push_back(dist(gen));
        }
        
        cols.push_back(std::move(colData));
    }
    
    return makeDataframe<Int>(mem, dfman, std::move(cols));
}

}
