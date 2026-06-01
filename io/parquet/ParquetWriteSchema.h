#pragma once

#include <stddef.h>

#include <string>
#include <string_view>
#include <vector>

namespace db {

// Physical column kinds a ParquetWriter can emit. UInt64 shares the INT64
// physical type with Int64 but is annotated UINT_64 so the on-disk statistics
// use unsigned ordering; full-range values round-trip via a bit-preserving
// reinterpret at the writeInt64Column boundary.
enum class ParquetColumnType {
    Int64,
    UInt64,
    Double,
    Bool,
    String,
    FixedLenBytes,
};

// Arrow-free description of an output Parquet schema: an ordered list of named,
// typed columns. ParquetWriter translates this into parquet schema nodes, so
// storage-side adapters can build schemas without pulling an Arrow include.
class ParquetWriteSchema {
public:
    ParquetWriteSchema();
    ~ParquetWriteSchema();

    ParquetWriteSchema(const ParquetWriteSchema&) = delete;
    ParquetWriteSchema(ParquetWriteSchema&&) = delete;
    ParquetWriteSchema& operator=(const ParquetWriteSchema&) = delete;
    ParquetWriteSchema& operator=(ParquetWriteSchema&&) = delete;

    void addColumn(std::string_view name, ParquetColumnType type);
    void addFixedLenColumn(std::string_view name, size_t byteWidth);

    size_t getColumnCount() const { return _columns.size(); }
    const std::string& getColumnName(size_t index) const;
    ParquetColumnType getColumnType(size_t index) const;
    size_t getColumnByteWidth(size_t index) const;

private:
    struct ParquetSchemaColumn {
        std::string _name;
        ParquetColumnType _type {ParquetColumnType::Int64};
        size_t _byteWidth {0};
    };

    std::vector<ParquetSchemaColumn> _columns;
};

}
