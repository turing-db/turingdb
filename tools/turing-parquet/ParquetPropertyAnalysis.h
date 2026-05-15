#pragma once

#include <stddef.h>

#include <array>
#include <map>
#include <memory>
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

// Per-key entry in the property analysis. _valueType is the first non-null
// JSON value type observed for this key; _isNullable becomes true if at least
// one NIL value was observed; _isMixed becomes true if at least two distinct
// non-NIL types were observed. If only NIL values were ever seen, _valueType
// remains NIL.
class ParquetPropertyType {
public:
    ParquetPropertyType();
    ~ParquetPropertyType();

    ParquetPropertyType(const ParquetPropertyType&) = delete;
    ParquetPropertyType(ParquetPropertyType&&) = delete;
    ParquetPropertyType& operator=(const ParquetPropertyType&) = delete;
    ParquetPropertyType& operator=(ParquetPropertyType&&) = delete;

    const std::string& getName() const { return _name; }
    ParquetJsonValueType getValueType() const { return _valueType; }
    bool isNullable() const { return _isNullable; }
    bool isMixed() const { return _isMixed; }
    size_t getCount() const { return _count; }

    void setName(const std::string& name) { _name = name; }
    void recordValue(ParquetJsonValueType type);

private:
    std::string _name;
    ParquetJsonValueType _valueType {ParquetJsonValueType::NIL};
    bool _hasNonNullValue {false};
    bool _isNullable {false};
    bool _isMixed {false};
    size_t _count {0};
};

// Aggregated view of a key-value JSON column's value types and a small set of
// previews for the structured (array/object) values encountered. Populated by
// ParquetPropertyAnalyzer.
class ParquetPropertyAnalysis {
public:
    static constexpr size_t TYPE_COUNT = 7;
    static constexpr size_t MAX_PREVIEWS = 5;

    using PropertyTypeMap = std::map<std::string, std::unique_ptr<ParquetPropertyType>>;

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

    const PropertyTypeMap& getPropertyTypes() const { return _propertyTypes; }

    void recordValue(ParquetJsonValueType type);
    void recordArrayPreview(const std::string& preview);
    void recordObjectPreview(const std::string& preview);

    ParquetPropertyType& getOrCreatePropertyType(const std::string& name);

    static const char* toString(ParquetJsonValueType type);

private:
    std::array<size_t, TYPE_COUNT> _typeCounts {};
    size_t _totalCount {0};
    std::vector<std::string> _arrayPreviews;
    std::vector<std::string> _objectPreviews;
    PropertyTypeMap _propertyTypes;
};

}
