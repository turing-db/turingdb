#include "ParquetWriteSchema.h"

#include "BioAssert.h"

using namespace db;

ParquetWriteSchema::ParquetWriteSchema() {
}

ParquetWriteSchema::~ParquetWriteSchema() {
}

void ParquetWriteSchema::addColumn(std::string_view name, ParquetColumnType type) {
    bioassert(type != ParquetColumnType::FixedLenBytes,
              "ParquetWriteSchema: use addFixedLenColumn for FixedLenBytes columns");

    _columns.emplace_back();
    ParquetSchemaColumn& column = _columns.back();
    column._name = std::string(name);
    column._type = type;
    column._byteWidth = 0;
}

void ParquetWriteSchema::addFixedLenColumn(std::string_view name, size_t byteWidth) {
    bioassert(byteWidth > 0,
              "ParquetWriteSchema: FixedLenBytes column needs a positive byteWidth");

    _columns.emplace_back();
    ParquetSchemaColumn& column = _columns.back();
    column._name = std::string(name);
    column._type = ParquetColumnType::FixedLenBytes;
    column._byteWidth = byteWidth;
}

const std::string& ParquetWriteSchema::getColumnName(size_t index) const {
    bioassert(index < _columns.size(), "ParquetWriteSchema: column index out of range");
    return _columns[index]._name;
}

ParquetColumnType ParquetWriteSchema::getColumnType(size_t index) const {
    bioassert(index < _columns.size(), "ParquetWriteSchema: column index out of range");
    return _columns[index]._type;
}

size_t ParquetWriteSchema::getColumnByteWidth(size_t index) const {
    bioassert(index < _columns.size(), "ParquetWriteSchema: column index out of range");
    return _columns[index]._byteWidth;
}
