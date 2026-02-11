#include <memory>

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

// check each column in the df to see if its sorted
bool isSorted(const Df& df) {
    const size_t numRows = df->getRowCount();
    if (numRows <= 1) return true;
    
    const auto& cols = df->cols();
    
    // Compare each adjacent pair of rows lexicographically
    for (size_t row = 0; row < numRows - 1; row++) {
        // Compare row with row+1 across all columns
        for (NamedColumn* ncol : cols) {
            auto* ccol = ncol->as<ColumnInts>();
            Int val1 = (*ccol)[row];
            Int val2 = (*ccol)[row + 1];
            
            if (val1 < val2) {
                // row < row+1, this is correct ordering, check next pair
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

// checks if a (presort) DF contains same elements as b (post sort)
bool containSame(const Df& a, const Df& b) {
    const auto& aCols = a->cols();
    const auto& bCols = b->cols();
    
    // Must have same number of columns
    if (aCols.size() != bCols.size()) return false;
    
    // Must have same number of rows
    if (a->getRowCount() != b->getRowCount()) return false;
    
    // Check each column contains same values (possibly in different order)
    for (size_t i = 0; i < aCols.size(); i++) {
        auto* aCol = aCols[i]->as<ColumnInts>();
        auto* bCol = bCols[i]->as<ColumnInts>();
        
        // Copy the data from both columns
        std::vector<Int> aData = aCol->getRaw();
        std::vector<Int> bData = bCol->getRaw();
        
        // Sort both copies
        std::ranges::sort(aData);
        std::ranges::sort(bData);
        
        // They should be identical if they contain the same multiset
        if (aData != bData) {
            return false;
        }
    }
    
    return true;
}

}
