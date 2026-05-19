#pragma once

#include <stddef.h>

#include <string>
#include <vector>

#include "ParquetReader.h"

namespace db {

class ParquetGraphImporter;
class ParquetSchema;

// SAX visitor that buffers per-chunk values for the canonical edge columns
// (`from`, `to`, the importer's edge-type column, and its property column),
// and on each chunk end iterates row-by-row, calling
// ParquetGraphImporter::onEdgeRow. The four columns must all be top-level
// BYTE_ARRAY primitives; the importer's own validation guarantees this
// before construction.
class ParquetEdgeRowVisitor : public ParquetSaxVisitor {
public:
    ParquetEdgeRowVisitor(const ParquetSchema& schema,
                          const std::string& edgeTypeColumn,
                          const std::string& propertyColumn,
                          ParquetGraphImporter& importer);
    ~ParquetEdgeRowVisitor() override;

    ParquetEdgeRowVisitor(const ParquetEdgeRowVisitor&) = delete;
    ParquetEdgeRowVisitor(ParquetEdgeRowVisitor&&) = delete;
    ParquetEdgeRowVisitor& operator=(const ParquetEdgeRowVisitor&) = delete;
    ParquetEdgeRowVisitor& operator=(ParquetEdgeRowVisitor&&) = delete;

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override;
    bool onChunkEnd(size_t rowGroupIndex,
                    size_t firstRowInRowGroup,
                    size_t rows) override;

private:
    ParquetGraphImporter& _importer;
    size_t _fromColumnIndex {0};
    size_t _toColumnIndex {0};
    size_t _edgeTypeColumnIndex {0};
    size_t _propertiesColumnIndex {0};

    std::vector<std::string> _fromIds;
    std::vector<std::string> _toIds;
    std::vector<std::string> _edgeTypes;
    std::vector<std::string> _properties;
};

}
