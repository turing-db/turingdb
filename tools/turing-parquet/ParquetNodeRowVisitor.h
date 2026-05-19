#pragma once

#include <stddef.h>

#include <string>
#include <vector>

#include "ParquetReader.h"

namespace db {

class ParquetGraphImporter;
class ParquetSchema;

// SAX visitor that buffers per-chunk values for the canonical node columns
// (`id`, `label`, and the importer's property column), and on each chunk end
// iterates row-by-row, calling ParquetGraphImporter::onNodeRow. The three
// columns are required to be top-level BYTE_ARRAY primitives; the importer's
// own validation guarantees this before construction.
class ParquetNodeRowVisitor : public ParquetSaxVisitor {
public:
    ParquetNodeRowVisitor(const ParquetSchema& schema,
                          const std::string& propertyColumn,
                          ParquetGraphImporter& importer);
    ~ParquetNodeRowVisitor() override;

    ParquetNodeRowVisitor(const ParquetNodeRowVisitor&) = delete;
    ParquetNodeRowVisitor(ParquetNodeRowVisitor&&) = delete;
    ParquetNodeRowVisitor& operator=(const ParquetNodeRowVisitor&) = delete;
    ParquetNodeRowVisitor& operator=(ParquetNodeRowVisitor&&) = delete;

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override;
    bool onChunkEnd(size_t rowGroupIndex,
                    size_t firstRowInRowGroup,
                    size_t rows) override;

private:
    ParquetGraphImporter& _importer;
    size_t _idColumnIndex {0};
    size_t _labelColumnIndex {0};
    size_t _propertiesColumnIndex {0};

    std::vector<std::string> _ids;
    std::vector<std::string> _labels;
    std::vector<std::string> _properties;
};

}
