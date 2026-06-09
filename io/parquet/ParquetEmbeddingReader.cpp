#include "ParquetEmbeddingReader.h"

#include <stddef.h>
#include <stdint.h>

#include <span>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"

#include "BioAssert.h"
#include "TuringException.h"

using namespace db;

namespace {

using EmbeddingElement = ParquetEmbeddingData::EmbeddingElement;

class ParquetEmbeddingVisitor : public ParquetSaxVisitor {
public:
    ParquetEmbeddingVisitor(std::string_view nodeIdColumn,
                            std::string_view embeddingColumn,
                            ParquetEmbeddingData* out)
        : _nodeIdColumn(nodeIdColumn),
        _embeddingColumn(embeddingColumn),
        _out(out)
    {
    }

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const parquet::SchemaDescriptor* schema = metadata.schema();
        const size_t columnCount = static_cast<size_t>(metadata.num_columns());

        for (size_t columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
            const parquet::ColumnDescriptor* descriptor = schema->Column(static_cast<int>(columnIndex));
            const std::string& name = descriptor->name();

            if (name == _nodeIdColumn) {
                const bool isInt64 = descriptor->physical_type() == parquet::Type::INT64;
                if (!isInt64) {
                    throw TuringException(fmt::format(
                        "LOAD EMBEDDING: node id column '{}' must be INT64", _nodeIdColumn));
                }

                _nodeIdColumnIndex = columnIndex;
            } else if (name == _embeddingColumn) {
                const bool isFixedLen = descriptor->physical_type() == parquet::Type::FIXED_LEN_BYTE_ARRAY;
                if (!isFixedLen) {
                    throw TuringException(fmt::format(
                        "LOAD EMBEDDING: embedding column '{}' must be FIXED_LEN_BYTE_ARRAY",
                        _embeddingColumn));
                }

                const size_t byteWidth = static_cast<size_t>(descriptor->type_length());
                if (byteWidth == 0 || byteWidth % sizeof(EmbeddingElement) != 0) {
                    throw TuringException(fmt::format(
                        "LOAD EMBEDDING: embedding column '{}' byte width {} is not a multiple of {}",
                        _embeddingColumn, byteWidth, sizeof(EmbeddingElement)));
                }

                _embeddingColumnIndex = columnIndex;
                _out->_dimension = byteWidth / sizeof(EmbeddingElement);
            }
        }

        if (_nodeIdColumnIndex == invalidColumn) {
            throw TuringException(fmt::format(
                "LOAD EMBEDDING: file is missing node id column '{}'", _nodeIdColumn));
        }

        if (_embeddingColumnIndex == invalidColumn) {
            throw TuringException(fmt::format(
                "LOAD EMBEDDING: file is missing embedding column '{}'", _embeddingColumn));
        }

        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex == _nodeIdColumnIndex) {
            _out->_nodeIDs.insert(_out->_nodeIDs.end(), values.begin(), values.end());
        }

        return true;
    }

    bool onFixedLenByteArrayValues(size_t columnIndex,
                                   std::span<const parquet::FixedLenByteArray> values,
                                   size_t byteWidth) override {
        if (columnIndex != _embeddingColumnIndex) {
            return true;
        }

        const size_t elementsPerValue = byteWidth / sizeof(EmbeddingElement);
        for (const parquet::FixedLenByteArray& value : values) {
            // Little-endian float32 payload, packed contiguously by the producer.
            // memcpy (not a reinterpret_cast to EmbeddingElement*) because value.ptr
            // has no alignment guarantee.
            std::vector<EmbeddingElement>& embeddingVector = _out->_embeddings.emplace_back(elementsPerValue);
            std::memcpy(embeddingVector.data(), value.ptr, byteWidth);
        }

        return true;
    }

private:
    static constexpr size_t invalidColumn = static_cast<size_t>(-1);

    std::string_view _nodeIdColumn;
    std::string_view _embeddingColumn;
    ParquetEmbeddingData* _out {nullptr};

    size_t _nodeIdColumnIndex {invalidColumn};
    size_t _embeddingColumnIndex {invalidColumn};
};

}

void ParquetEmbeddingReader::read(const fs::Path& path,
                                  std::string_view nodeIdColumn,
                                  std::string_view embeddingColumn,
                                  ParquetEmbeddingData* out) {
    bioassert(out, "ParquetEmbeddingReader::read requires a non-null output");

    ParquetEmbeddingVisitor visitor(nodeIdColumn, embeddingColumn, out);
    ParquetReader reader(path, visitor);

    while (reader.nextChunk()) {
    }

    if (out->_embeddings.size() != out->_nodeIDs.size()) {
        throw TuringException(fmt::format(
            "LOAD EMBEDDING: node id count {} does not match embedding count {} at dimension {}",
            out->_nodeIDs.size(),
            out->_embeddings.size(),
            out->_dimension));
    }
}
