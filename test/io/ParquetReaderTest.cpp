#include "TuringTest.h"

#include "ParquetReader.h"

#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "Path.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

struct RecordedEvent {
    enum Kind {
        FileStart,
        RowGroupStart,
        ColumnStart,
        ChunkEnd,
        ColumnEnd,
        RowGroupEnd,
        FileEnd,
    };

    Kind _kind {FileStart};
    size_t _rowGroup {0};
    size_t _column {0};
    int64_t _numRows {0};
    std::string _columnName;
    parquet::Type::type _physicalType {parquet::Type::BOOLEAN};
};

class RecordingVisitor : public ParquetSaxVisitor {
public:
    std::vector<RecordedEvent> _events;
    std::vector<int64_t> _idValues;
    std::vector<std::string> _nameValues;

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        _events.push_back({RecordedEvent::FileStart, 0, 0, 0, "", parquet::Type::BOOLEAN});
        return true;
    }

    bool onRowGroupStart(size_t rowGroupIndex,
                         const parquet::RowGroupMetaData& metadata) override {
        _events.push_back({RecordedEvent::RowGroupStart, rowGroupIndex, 0,
                           metadata.num_rows(), "", parquet::Type::BOOLEAN});
        return true;
    }

    bool onColumnStart(size_t rowGroupIndex,
                       size_t columnIndex,
                       const parquet::ColumnDescriptor& descriptor) override {
        _events.push_back({RecordedEvent::ColumnStart, rowGroupIndex, columnIndex,
                           0, descriptor.name(), descriptor.physical_type()});
        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t v : values) {
            _idValues.push_back(v);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        for (const auto& byteArray : values) {
            _nameValues.emplace_back(
                reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }

    bool onChunkEnd(size_t rowGroupIndex,
                    size_t firstRowInRowGroup,
                    size_t rows) override {
        _events.push_back({RecordedEvent::ChunkEnd, rowGroupIndex, 0,
                           static_cast<int64_t>(rows), "", parquet::Type::BOOLEAN});
        return true;
    }

    bool onColumnEnd(size_t rowGroupIndex, size_t columnIndex) override {
        _events.push_back({RecordedEvent::ColumnEnd, rowGroupIndex, columnIndex, 0, "",
                           parquet::Type::BOOLEAN});
        return true;
    }

    bool onRowGroupEnd(size_t rowGroupIndex) override {
        _events.push_back({RecordedEvent::RowGroupEnd, rowGroupIndex, 0, 0, "",
                           parquet::Type::BOOLEAN});
        return true;
    }

    bool onFileEnd() override {
        _events.push_back({RecordedEvent::FileEnd, 0, 0, 0, "", parquet::Type::BOOLEAN});
        return true;
    }
};

static void drain(ParquetReader& reader, size_t maxRows = ParquetReader::DEFAULT_CHUNK_SIZE) {
    while (reader.nextChunk(maxRows)) {
    }
}

class ParquetReaderTest : public TuringTest {
protected:
    std::string _parquetPath;

    void initialize() override {
        _parquetPath = _outDir + "/test.parquet";
    }

    void writeTestParquet(const std::vector<int64_t>& ids,
                          const std::vector<std::string>& names) {
        ASSERT_EQ(ids.size(), names.size());

        parquet::schema::NodeVector fields;
        fields.push_back(parquet::schema::PrimitiveNode::Make(
            "id", parquet::Repetition::REQUIRED, parquet::Type::INT64));
        fields.push_back(parquet::schema::PrimitiveNode::Make(
            "name", parquet::Repetition::REQUIRED,
            parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8));

        const auto schemaNode = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make(
                "schema", parquet::Repetition::REQUIRED, fields));

        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::UNCOMPRESSED);
        const auto props = builder.build();

        const auto outFileSp =
            arrow::io::FileOutputStream::Open(_parquetPath).ValueOrDie();
        auto writer = parquet::ParquetFileWriter::Open(outFileSp, schemaNode, props);

        parquet::RowGroupWriter* rgWriter = writer->AppendRowGroup();

        auto* idWriter = static_cast<parquet::Int64Writer*>(rgWriter->NextColumn());
        idWriter->WriteBatch(static_cast<int64_t>(ids.size()),
                             nullptr, nullptr, ids.data());

        std::vector<parquet::ByteArray> nameBA;
        nameBA.reserve(names.size());
        for (const auto& n : names) {
            nameBA.emplace_back(static_cast<uint32_t>(n.size()),
                                reinterpret_cast<const uint8_t*>(n.data()));
        }

        auto* nameWriter = static_cast<parquet::ByteArrayWriter*>(rgWriter->NextColumn());
        nameWriter->WriteBatch(static_cast<int64_t>(nameBA.size()),
                               nullptr, nullptr, nameBA.data());

        writer->Close();
    }
};

TEST_F(ParquetReaderTest, FiresFullEventSequence) {
    const std::vector<int64_t> ids {1, 2, 3};
    const std::vector<std::string> names {"alice", "bob", "carol"};
    writeTestParquet(ids, names);

    RecordingVisitor visitor;
    ParquetReader reader(fs::Path(_parquetPath), visitor);
    drain(reader);

    // Expected: FileStart, RowGroupStart,
    //           ColumnStart(0,id), ColumnStart(1,name),
    //           ChunkEnd(rows=3),
    //           ColumnEnd(0), ColumnEnd(1),
    //           RowGroupEnd, FileEnd
    ASSERT_EQ(visitor._events.size(), 9u);

    EXPECT_EQ(visitor._events[0]._kind, RecordedEvent::FileStart);

    EXPECT_EQ(visitor._events[1]._kind, RecordedEvent::RowGroupStart);
    EXPECT_EQ(visitor._events[1]._rowGroup, 0u);
    EXPECT_EQ(visitor._events[1]._numRows, 3);

    EXPECT_EQ(visitor._events[2]._kind, RecordedEvent::ColumnStart);
    EXPECT_EQ(visitor._events[2]._column, 0u);
    EXPECT_EQ(visitor._events[2]._columnName, "id");
    EXPECT_EQ(visitor._events[2]._physicalType, parquet::Type::INT64);

    EXPECT_EQ(visitor._events[3]._kind, RecordedEvent::ColumnStart);
    EXPECT_EQ(visitor._events[3]._column, 1u);
    EXPECT_EQ(visitor._events[3]._columnName, "name");
    EXPECT_EQ(visitor._events[3]._physicalType, parquet::Type::BYTE_ARRAY);

    EXPECT_EQ(visitor._events[4]._kind, RecordedEvent::ChunkEnd);
    EXPECT_EQ(visitor._events[4]._numRows, 3);

    EXPECT_EQ(visitor._events[5]._kind, RecordedEvent::ColumnEnd);
    EXPECT_EQ(visitor._events[5]._column, 0u);

    EXPECT_EQ(visitor._events[6]._kind, RecordedEvent::ColumnEnd);
    EXPECT_EQ(visitor._events[6]._column, 1u);

    EXPECT_EQ(visitor._events[7]._kind, RecordedEvent::RowGroupEnd);
    EXPECT_EQ(visitor._events[8]._kind, RecordedEvent::FileEnd);

    EXPECT_EQ(visitor._idValues, ids);
    EXPECT_EQ(visitor._nameValues, names);
}

TEST_F(ParquetReaderTest, ColumnProjectionSkipsUnselectedColumns) {
    const std::vector<int64_t> ids {10, 20, 30};
    const std::vector<std::string> names {"a", "b", "c"};
    writeTestParquet(ids, names);

    RecordingVisitor visitor;
    ParquetReader reader(fs::Path(_parquetPath), visitor);
    reader.setColumnProjection({0});
    drain(reader);

    size_t columnStartEvents = 0;
    for (const auto& e : visitor._events) {
        if (e._kind == RecordedEvent::ColumnStart) {
            ++columnStartEvents;
        }
    }

    EXPECT_EQ(columnStartEvents, 1u);
    EXPECT_EQ(visitor._idValues, ids);
    EXPECT_TRUE(visitor._nameValues.empty());
}

class AbortingVisitor : public ParquetSaxVisitor {
public:
    int _columnStartsSeen {0};

    bool onColumnStart(size_t rowGroupIndex,
                       size_t columnIndex,
                       const parquet::ColumnDescriptor& descriptor) override {
        ++_columnStartsSeen;
        return false;
    }
};

TEST_F(ParquetReaderTest, ReturningFalseAbortsRead) {
    writeTestParquet({1, 2, 3}, {"a", "b", "c"});

    AbortingVisitor visitor;
    ParquetReader reader(fs::Path(_parquetPath), visitor);
    drain(reader);

    EXPECT_EQ(visitor._columnStartsSeen, 1);
}

TEST_F(ParquetReaderTest, ThrowsOnMissingFile) {
    RecordingVisitor visitor;
    ParquetReader reader(fs::Path(_outDir + "/does-not-exist.parquet"), visitor);
    EXPECT_THROW(reader.nextChunk(), TuringException);
}

class ChunkSizeVisitor : public ParquetSaxVisitor {
public:
    std::vector<size_t> _chunkRows;
    std::vector<int64_t> _idValues;
    std::vector<std::string> _nameValues;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t v : values) {
            _idValues.push_back(v);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        for (const auto& byteArray : values) {
            _nameValues.emplace_back(
                reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }

    bool onChunkEnd(size_t rowGroupIndex,
                    size_t firstRowInRowGroup,
                    size_t rows) override {
        _chunkRows.push_back(rows);
        return true;
    }
};

TEST_F(ParquetReaderTest, NextChunkHonorsRequestedSize) {
    constexpr size_t totalRows = 2500;
    std::vector<int64_t> ids;
    std::vector<std::string> names;
    ids.reserve(totalRows);
    names.reserve(totalRows);
    for (size_t i = 0; i < totalRows; ++i) {
        ids.push_back(static_cast<int64_t>(i));
        names.push_back(fmt::format("n{}", i));
    }
    writeTestParquet(ids, names);

    ChunkSizeVisitor visitor;
    ParquetReader reader(fs::Path(_parquetPath), visitor);

    constexpr size_t chunkSize = 1000;
    drain(reader, chunkSize);

    size_t total = 0;
    for (const size_t rows : visitor._chunkRows) {
        EXPECT_LE(rows, chunkSize);
        EXPECT_GT(rows, 0u);
        total += rows;
    }
    EXPECT_EQ(total, totalRows);
    EXPECT_EQ(visitor._idValues, ids);
    EXPECT_EQ(visitor._nameValues, names);
}
