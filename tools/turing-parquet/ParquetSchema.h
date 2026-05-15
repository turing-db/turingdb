#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

namespace db {

enum class ParquetFieldRepetition {
    REQUIRED,
    OPTIONAL,
    REPEATED,
    UNDEFINED,
};

enum class ParquetPrimitiveType {
    BOOLEAN,
    INT32,
    INT64,
    INT96,
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY,
};

// A node in the Parquet schema tree, either a primitive leaf or a group with
// children.
class ParquetSchemaField {
public:
    ParquetSchemaField();
    ~ParquetSchemaField();

    ParquetSchemaField(const ParquetSchemaField&) = delete;
    ParquetSchemaField(ParquetSchemaField&&) = delete;
    ParquetSchemaField& operator=(const ParquetSchemaField&) = delete;
    ParquetSchemaField& operator=(ParquetSchemaField&&) = delete;

    const std::string& getName() const { return _name; }
    ParquetFieldRepetition getRepetition() const { return _repetition; }
    const std::string& getLogicalType() const { return _logicalType; }
    bool isGroup() const { return _isGroup; }

    // Primitive-only accessors. getFixedLength is only meaningful when the
    // primitive type is FIXED_LEN_BYTE_ARRAY.
    ParquetPrimitiveType getPrimitiveType() const { return _primitiveType; }
    size_t getFixedLength() const { return _fixedLength; }

    // Group-only accessors.
    size_t getChildCount() const { return _children.size(); }
    const ParquetSchemaField& getChild(size_t index) const { return *_children[index]; }

    void setName(const std::string& name) { _name = name; }
    void setRepetition(ParquetFieldRepetition repetition) { _repetition = repetition; }
    void setLogicalType(const std::string& logicalType) { _logicalType = logicalType; }
    void setPrimitiveType(ParquetPrimitiveType primitiveType) { _primitiveType = primitiveType; }
    void setFixedLength(size_t fixedLength) { _fixedLength = fixedLength; }
    void markAsGroup() { _isGroup = true; }

    ParquetSchemaField& addChild();

private:
    std::string _name;
    ParquetFieldRepetition _repetition {ParquetFieldRepetition::UNDEFINED};
    std::string _logicalType;
    bool _isGroup {false};
    ParquetPrimitiveType _primitiveType {ParquetPrimitiveType::BOOLEAN};
    size_t _fixedLength {0};
    std::vector<std::unique_ptr<ParquetSchemaField>> _children;
};

// High-level view of a Parquet file's schema and basic file metadata. Built by
// ParquetSchemaExtractor and consumed by tools that need a parquet-independent
// representation of the schema.
class ParquetSchema {
public:
    ParquetSchema();
    ~ParquetSchema();

    ParquetSchema(const ParquetSchema&) = delete;
    ParquetSchema(ParquetSchema&&) = delete;
    ParquetSchema& operator=(const ParquetSchema&) = delete;
    ParquetSchema& operator=(ParquetSchema&&) = delete;

    const std::string& getCreatedBy() const { return _createdBy; }
    const std::string& getVersion() const { return _version; }
    size_t getRowCount() const { return _rowCount; }
    size_t getRowGroupCount() const { return _rowGroupCount; }
    size_t getColumnCount() const { return _columnCount; }

    const ParquetSchemaField& getRoot() const { return _root; }
    ParquetSchemaField& getRoot() { return _root; }

    void setCreatedBy(const std::string& createdBy) { _createdBy = createdBy; }
    void setVersion(const std::string& version) { _version = version; }
    void setRowCount(size_t rowCount) { _rowCount = rowCount; }
    void setRowGroupCount(size_t rowGroupCount) { _rowGroupCount = rowGroupCount; }
    void setColumnCount(size_t columnCount) { _columnCount = columnCount; }

    static const char* toString(ParquetFieldRepetition repetition);
    static const char* toString(ParquetPrimitiveType primitiveType);

private:
    std::string _createdBy;
    std::string _version;
    size_t _rowCount {0};
    size_t _rowGroupCount {0};
    size_t _columnCount {0};
    ParquetSchemaField _root;
};

}
