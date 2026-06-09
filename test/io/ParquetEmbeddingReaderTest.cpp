#include "TuringTest.h"

#include "ParquetEmbeddingReader.h"

#include <stdint.h>

#include <string>
#include <vector>

#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "Path.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view nodeIdColumn = "node_id";
constexpr std::string_view embeddingColumn = "embedding";

parquet::schema::NodePtr int64Field(const std::string& name) {
    return parquet::schema::PrimitiveNode::Make(
        name, parquet::Repetition::REQUIRED, parquet::Type::INT64);
}

parquet::schema::NodePtr doubleField(const std::string& name) {
    return parquet::schema::PrimitiveNode::Make(
        name, parquet::Repetition::REQUIRED, parquet::Type::DOUBLE);
}

parquet::schema::NodePtr fixedLenField(const std::string& name, int byteWidth) {
    return parquet::schema::PrimitiveNode::Make(
        name, parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
        parquet::ConvertedType::NONE, byteWidth);
}

}

class ParquetEmbeddingReaderTest : public TuringTest {
protected:
    std::string _path;

    void initialize() override {
        _path = _outDir + "/embeddings.parquet";
    }

    // Opens a writer over _path for the given schema fields, hands the row group
    // to writeColumns, and closes the file.
    void writeParquet(const parquet::schema::NodeVector& fields,
                      const std::function<void(parquet::RowGroupWriter*)>& writeColumns) {
        const auto schemaNode = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make(
                "schema", parquet::Repetition::REQUIRED, fields));

        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::UNCOMPRESSED);

        const auto outFile = arrow::io::FileOutputStream::Open(_path).ValueOrDie();
        auto writer = parquet::ParquetFileWriter::Open(outFile, schemaNode, builder.build());

        parquet::RowGroupWriter* rowGroup = writer->AppendRowGroup();
        writeColumns(rowGroup);
        writer->Close();
    }

    static void writeInt64Column(parquet::RowGroupWriter* rowGroup,
                                 const std::vector<int64_t>& values) {
        auto* writer = static_cast<parquet::Int64Writer*>(rowGroup->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(values.size()), nullptr, nullptr, values.data());
    }

    // Writes a FIXED_LEN_BYTE_ARRAY column from contiguous bytes; each value is
    // byteWidth bytes wide. The backing buffer must outlive this call.
    static void writeFixedLenColumn(parquet::RowGroupWriter* rowGroup,
                                    const std::vector<uint8_t>& bytes,
                                    size_t rows,
                                    size_t byteWidth) {
        std::vector<parquet::FixedLenByteArray> values(rows);
        for (size_t row = 0; row < rows; ++row) {
            values[row].ptr = bytes.data() + row * byteWidth;
        }

        auto* writer = static_cast<parquet::FixedLenByteArrayWriter*>(rowGroup->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(rows), nullptr, nullptr, values.data());
    }

    // Packs row-major float vectors into a little-endian byte buffer.
    static std::vector<uint8_t> packFloats(const std::vector<std::vector<float>>& vectors) {
        std::vector<uint8_t> bytes;
        for (const auto& vector : vectors) {
            const auto* raw = reinterpret_cast<const uint8_t*>(vector.data());
            bytes.insert(bytes.end(), raw, raw + vector.size() * sizeof(float));
        }
        return bytes;
    }

    // Writes a well-formed embedding file: INT64 node_id + FIXED_LEN_BYTE_ARRAY embedding.
    void writeValidFile(const std::vector<int64_t>& ids,
                        const std::vector<std::vector<float>>& vectors) {
        const size_t dimension = vectors.front().size();
        const std::vector<uint8_t> bytes = packFloats(vectors);

        parquet::schema::NodeVector fields;
        fields.push_back(int64Field(std::string {nodeIdColumn}));
        fields.push_back(fixedLenField(std::string {embeddingColumn},
                                       static_cast<int>(dimension * sizeof(float))));

        writeParquet(fields, [&](parquet::RowGroupWriter* rowGroup) {
            writeInt64Column(rowGroup, ids);
            writeFixedLenColumn(rowGroup, bytes, ids.size(), dimension * sizeof(float));
        });
    }
};

TEST_F(ParquetEmbeddingReaderTest, ReadsValidFile) {
    const std::vector<int64_t> ids {0, 1, 2};
    const std::vector<std::vector<float>> vectors {
        {0.1f, 0.2f, 0.3f, 0.4f},
        {0.5f, 0.6f, 0.7f, 0.8f},
        {0.9f, 1.0f, 1.1f, 1.2f},
    };
    writeValidFile(ids, vectors);

    ParquetEmbeddingData data;
    ParquetEmbeddingReader::read(fs::Path(_path), nodeIdColumn, embeddingColumn, &data);

    EXPECT_EQ(data._dimension, 4u);
    EXPECT_EQ(data._nodeIDs, ids);
    ASSERT_EQ(data._embeddings.size(), vectors.size());
    for (size_t row = 0; row < vectors.size(); ++row) {
        ASSERT_EQ(data._embeddings[row].size(), data._dimension);
        for (size_t col = 0; col < vectors[row].size(); ++col) {
            EXPECT_FLOAT_EQ(data._embeddings[row][col], vectors[row][col]);
        }
    }
}

TEST_F(ParquetEmbeddingReaderTest, ThrowsOnMissingFile) {
    ParquetEmbeddingData data;
    EXPECT_THROW(
        ParquetEmbeddingReader::read(
            fs::Path(_outDir + "/does-not-exist.parquet"), nodeIdColumn, embeddingColumn, &data),
        TuringException);
}

TEST_F(ParquetEmbeddingReaderTest, ThrowsWhenNodeIdColumnMissing) {
    const std::vector<std::vector<float>> vectors {{0.1f, 0.2f}};
    const std::vector<uint8_t> bytes = packFloats(vectors);

    parquet::schema::NodeVector fields;
    fields.push_back(fixedLenField(std::string {embeddingColumn}, 2 * sizeof(float)));
    writeParquet(fields, [&](parquet::RowGroupWriter* rowGroup) {
        writeFixedLenColumn(rowGroup, bytes, 1, 2 * sizeof(float));
    });

    ParquetEmbeddingData data;
    EXPECT_THROW(
        ParquetEmbeddingReader::read(fs::Path(_path), nodeIdColumn, embeddingColumn, &data),
        TuringException);
}

TEST_F(ParquetEmbeddingReaderTest, ThrowsWhenEmbeddingColumnMissing) {
    parquet::schema::NodeVector fields;
    fields.push_back(int64Field(std::string {nodeIdColumn}));
    writeParquet(fields, [&](parquet::RowGroupWriter* rowGroup) {
        writeInt64Column(rowGroup, {0, 1, 2});
    });

    ParquetEmbeddingData data;
    EXPECT_THROW(
        ParquetEmbeddingReader::read(fs::Path(_path), nodeIdColumn, embeddingColumn, &data),
        TuringException);
}

TEST_F(ParquetEmbeddingReaderTest, ThrowsWhenNodeIdColumnNotInt64) {
    // node_id written as DOUBLE instead of INT64.
    const std::vector<std::vector<float>> vectors {{0.1f, 0.2f}};
    const std::vector<uint8_t> bytes = packFloats(vectors);
    const std::vector<double> ids {0.0};

    parquet::schema::NodeVector fields;
    fields.push_back(doubleField(std::string {nodeIdColumn}));
    fields.push_back(fixedLenField(std::string {embeddingColumn}, 2 * sizeof(float)));
    writeParquet(fields, [&](parquet::RowGroupWriter* rowGroup) {
        auto* writer = static_cast<parquet::DoubleWriter*>(rowGroup->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(ids.size()), nullptr, nullptr, ids.data());
        writeFixedLenColumn(rowGroup, bytes, 1, 2 * sizeof(float));
    });

    ParquetEmbeddingData data;
    EXPECT_THROW(
        ParquetEmbeddingReader::read(fs::Path(_path), nodeIdColumn, embeddingColumn, &data),
        TuringException);
}

TEST_F(ParquetEmbeddingReaderTest, ThrowsWhenEmbeddingColumnNotFixedLenByteArray) {
    // embedding written as DOUBLE instead of FIXED_LEN_BYTE_ARRAY.
    const std::vector<int64_t> ids {0};
    const std::vector<double> embedding {0.5};

    parquet::schema::NodeVector fields;
    fields.push_back(int64Field(std::string {nodeIdColumn}));
    fields.push_back(doubleField(std::string {embeddingColumn}));
    writeParquet(fields, [&](parquet::RowGroupWriter* rowGroup) {
        writeInt64Column(rowGroup, ids);
        auto* writer = static_cast<parquet::DoubleWriter*>(rowGroup->NextColumn());
        writer->WriteBatch(static_cast<int64_t>(embedding.size()), nullptr, nullptr, embedding.data());
    });

    ParquetEmbeddingData data;
    EXPECT_THROW(
        ParquetEmbeddingReader::read(fs::Path(_path), nodeIdColumn, embeddingColumn, &data),
        TuringException);
}

TEST_F(ParquetEmbeddingReaderTest, ThrowsWhenEmbeddingByteWidthNotMultipleOfFloat) {
    // FIXED_LEN_BYTE_ARRAY width of 15 is not a multiple of sizeof(float) == 4.
    constexpr size_t byteWidth = 15;
    const std::vector<int64_t> ids {0};
    const std::vector<uint8_t> bytes(byteWidth, 0);

    parquet::schema::NodeVector fields;
    fields.push_back(int64Field(std::string {nodeIdColumn}));
    fields.push_back(fixedLenField(std::string {embeddingColumn}, byteWidth));
    writeParquet(fields, [&](parquet::RowGroupWriter* rowGroup) {
        writeInt64Column(rowGroup, ids);
        writeFixedLenColumn(rowGroup, bytes, 1, byteWidth);
    });

    ParquetEmbeddingData data;
    EXPECT_THROW(
        ParquetEmbeddingReader::read(fs::Path(_path), nodeIdColumn, embeddingColumn, &data),
        TuringException);
}
