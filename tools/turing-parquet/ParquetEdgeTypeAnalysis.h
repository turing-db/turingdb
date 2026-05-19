#pragma once

#include <stddef.h>

#include <map>
#include <string>
#include <string_view>

namespace db {

// Histogram of edge-type label values observed in a single string column.
// Populated by ParquetEdgeTypeAnalyzer. A caller can accumulate counts across
// multiple parquet files by reusing the same instance — recordType() simply
// increments the per-value counter and the running total.
class ParquetEdgeTypeAnalysis {
public:
    using TypeCountMap = std::map<std::string, size_t>;

    ParquetEdgeTypeAnalysis();
    ~ParquetEdgeTypeAnalysis();

    ParquetEdgeTypeAnalysis(const ParquetEdgeTypeAnalysis&) = delete;
    ParquetEdgeTypeAnalysis(ParquetEdgeTypeAnalysis&&) = delete;
    ParquetEdgeTypeAnalysis& operator=(const ParquetEdgeTypeAnalysis&) = delete;
    ParquetEdgeTypeAnalysis& operator=(ParquetEdgeTypeAnalysis&&) = delete;

    const TypeCountMap& getTypeCounts() const { return _typeCounts; }
    size_t getTotalCount() const { return _totalCount; }

    void recordType(std::string_view value);

private:
    TypeCountMap _typeCounts;
    size_t _totalCount {0};
};

}
