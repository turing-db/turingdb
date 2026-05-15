#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

namespace db {

class ParquetPropertyAnalysis;

enum class ParquetTuringType {
    BOOLEAN,
    INTEGER,
    FLOAT,
    STRING,
};

enum class ParquetEdgeCardinality {
    ONE,
    MANY,
};

// A single TuringDB-style property attached to a label (or to the root). The
// _isRawJson flag means the value is stored as a string but holds raw JSON —
// emitted when the source JSON was mixed-type, always-null, contained nested
// arrays, or had empty arrays so the element type could not be inferred.
class ParquetGraphProperty {
public:
    ParquetGraphProperty();
    ~ParquetGraphProperty();

    ParquetGraphProperty(const ParquetGraphProperty&) = delete;
    ParquetGraphProperty(ParquetGraphProperty&&) = delete;
    ParquetGraphProperty& operator=(const ParquetGraphProperty&) = delete;
    ParquetGraphProperty& operator=(ParquetGraphProperty&&) = delete;

    const std::string& getName() const { return _name; }
    ParquetTuringType getType() const { return _type; }
    bool isNullable() const { return _isNullable; }
    bool isRawJson() const { return _isRawJson; }

    void setName(const std::string& name) { _name = name; }
    void setType(ParquetTuringType type) { _type = type; }
    void setNullable(bool isNullable) { _isNullable = isNullable; }
    void setRawJson(bool isRawJson) { _isRawJson = isRawJson; }

private:
    std::string _name;
    ParquetTuringType _type {ParquetTuringType::STRING};
    bool _isNullable {false};
    bool _isRawJson {false};
};

// A label discovered as a sub-record of its parent. The edge cardinality
// records whether the parent links to ONE such record (JSON object) or MANY
// (JSON array). The label carries its own properties and may itself contain
// further sub-labels for nested objects / arrays-of-objects.
class ParquetGraphLabel {
public:
    ParquetGraphLabel();
    ~ParquetGraphLabel();

    ParquetGraphLabel(const ParquetGraphLabel&) = delete;
    ParquetGraphLabel(ParquetGraphLabel&&) = delete;
    ParquetGraphLabel& operator=(const ParquetGraphLabel&) = delete;
    ParquetGraphLabel& operator=(ParquetGraphLabel&&) = delete;

    const std::string& getName() const { return _name; }
    ParquetEdgeCardinality getCardinality() const { return _cardinality; }
    bool isNullable() const { return _isNullable; }
    const std::string& getInferredLabel() const { return _inferredLabel; }
    const std::vector<std::unique_ptr<ParquetGraphProperty>>& getProperties() const { return _properties; }
    const std::vector<std::unique_ptr<ParquetGraphLabel>>& getSubLabels() const { return _subLabels; }

    void setName(const std::string& name) { _name = name; }
    void setCardinality(ParquetEdgeCardinality cardinality) { _cardinality = cardinality; }
    void setNullable(bool isNullable) { _isNullable = isNullable; }
    void setInferredLabel(const std::string& inferredLabel) { _inferredLabel = inferredLabel; }

    ParquetGraphProperty& addProperty();
    ParquetGraphLabel& addSubLabel();

private:
    std::string _name;
    ParquetEdgeCardinality _cardinality {ParquetEdgeCardinality::ONE};
    bool _isNullable {false};
    std::string _inferredLabel;
    std::vector<std::unique_ptr<ParquetGraphProperty>> _properties;
    std::vector<std::unique_ptr<ParquetGraphLabel>> _subLabels;
};

// The TuringDB graph schema derived from a JSON-typed Parquet column. The
// root holds the properties that live directly on the row's node and the
// labels of records linked from it. Warnings are accumulated for shapes that
// could not be mapped cleanly (mixed types, pure-null keys, empty-arrays
// with unknown element type, nested arrays).
class ParquetGraphMapping {
public:
    ParquetGraphMapping();
    ~ParquetGraphMapping();

    ParquetGraphMapping(const ParquetGraphMapping&) = delete;
    ParquetGraphMapping(ParquetGraphMapping&&) = delete;
    ParquetGraphMapping& operator=(const ParquetGraphMapping&) = delete;
    ParquetGraphMapping& operator=(ParquetGraphMapping&&) = delete;

    const std::string& getColumnName() const { return _columnName; }
    const ParquetGraphLabel& getRoot() const { return _root; }
    ParquetGraphLabel& getRoot() { return _root; }
    const std::vector<std::string>& getWarnings() const { return _warnings; }

    void setColumnName(const std::string& columnName) { _columnName = columnName; }
    void addWarning(const std::string& message) { _warnings.push_back(message); }

    static const char* toString(ParquetTuringType type);
    static const char* toString(ParquetEdgeCardinality cardinality);

    static void buildFrom(const ParquetPropertyAnalysis& analysis,
                          const std::string& columnName,
                          ParquetGraphMapping& mapping);

    // Walks every sub-label and sets its inferred TuringDB label name (e.g.
    // `associated_proteins` -> `AssociatedProtein`): non-alpha characters are
    // dropped as word separators, each word's first letter is capitalised, and
    // a trailing plural `s` is stripped (unless preceded by another `s`).
    static void inferLabelNames(ParquetGraphMapping& mapping);

private:
    std::string _columnName;
    ParquetGraphLabel _root;
    std::vector<std::string> _warnings;
};

}
