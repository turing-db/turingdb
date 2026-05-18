#pragma once

#include "ParquetPropertyAnalysis.h"

namespace db {

// Unions per-file ParquetPropertyAnalysis snapshots of the same JSON property
// column into a single combined analysis. Each call to merge() folds the
// source's type breakdown, property-type tree, and previews into the target
// the merger was constructed with. _valueType disagreements between source
// and target on the same property mark the combined entry as mixed; null
// observations and counts accumulate; sub-properties and array element types
// merge recursively. There are no structural conflicts to surface — this is
// purely an aggregation.
//
// Following the visitor pattern in this directory, the merged analysis is
// owned by the caller and passed by reference; ParquetPropertyMerge stores
// the reference and mutates the caller's analysis in merge().
class ParquetPropertyMerge {
public:
    ParquetPropertyMerge(ParquetPropertyAnalysis& merged);
    ~ParquetPropertyMerge();

    ParquetPropertyMerge(const ParquetPropertyMerge&) = delete;
    ParquetPropertyMerge(ParquetPropertyMerge&&) = delete;
    ParquetPropertyMerge& operator=(const ParquetPropertyMerge&) = delete;
    ParquetPropertyMerge& operator=(ParquetPropertyMerge&&) = delete;

    void merge(const ParquetPropertyAnalysis& source);

private:
    ParquetPropertyAnalysis& _merged;
};

}
