#pragma once

#include <stddef.h>

#include <array>
#include <string>
#include <vector>

namespace db {

enum class ParquetJsonValueType {
    NIL,
    BOOLEAN,
    INTEGER,
    FLOAT,
    STRING,
    ARRAY,
    OBJECT,
};

// Aggregated view of a key-value JSON column's value types and a small set of
// previews for the structured (array/object) values encountered. Populated by
// ParquetPropertyAnalyzer.
class ParquetPropertyAnalysis {
public:
    static constexpr size_t TYPE_COUNT = 7;
    static constexpr size_t MAX_PREVIEWS = 5;

    ParquetPropertyAnalysis();
    ~ParquetPropertyAnalysis();

    ParquetPropertyAnalysis(const ParquetPropertyAnalysis&) = delete;
    ParquetPropertyAnalysis(ParquetPropertyAnalysis&&) = delete;
    ParquetPropertyAnalysis& operator=(const ParquetPropertyAnalysis&) = delete;
    ParquetPropertyAnalysis& operator=(ParquetPropertyAnalysis&&) = delete;

    size_t getTypeCount(ParquetJsonValueType type) const;
    size_t getTotalCount() const { return _totalCount; }

    const std::vector<std::string>& getArrayPreviews() const { return _arrayPreviews; }
    const std::vector<std::string>& getObjectPreviews() const { return _objectPreviews; }

    void recordValue(ParquetJsonValueType type);
    void recordArrayPreview(const std::string& preview);
    void recordObjectPreview(const std::string& preview);

    static const char* toString(ParquetJsonValueType type);

private:
    std::array<size_t, TYPE_COUNT> _typeCounts {};
    size_t _totalCount {0};
    std::vector<std::string> _arrayPreviews;
    std::vector<std::string> _objectPreviews;
};

}
