#include "TuringTest.h"

#include "ParquetReader.h"

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

struct RecordedEvent {
    enum Kind {
        FileStart,
        RowGroupStart,
        ColumnStart,
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

    bool onInt64Values(size_t rowGroupIndex,
                       size_t columnIndex,
                       std::span<const int64_t> values) override {
        for (const int64_t v : values) {
            _idValues.push_back(v);
        }
        return true;
    }

    bool onByteArrayValues(size_t rowGroupIndex,
                           size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        for (const auto& ba : values) {
            _nameValues.emplace_back(reinterpret_cast<const char*>(ba.ptr), ba.len);
        }
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

        // arrow::io::FileOutputStream::Open returns shared_ptr; bind tight.
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
    reader.read();

    // Expected: FileStart, RowGroupStart,
    //           ColumnStart(0,id), ColumnEnd(0),
    //           ColumnStart(1,name), ColumnEnd(1),
    //           RowGroupEnd, FileEnd
    ASSERT_EQ(visitor._events.size(), 8u);

    EXPECT_EQ(visitor._events[0]._kind, RecordedEvent::FileStart);

    EXPECT_EQ(visitor._events[1]._kind, RecordedEvent::RowGroupStart);
    EXPECT_EQ(visitor._events[1]._rowGroup, 0u);
    EXPECT_EQ(visitor._events[1]._numRows, 3);

    EXPECT_EQ(visitor._events[2]._kind, RecordedEvent::ColumnStart);
    EXPECT_EQ(visitor._events[2]._column, 0u);
    EXPECT_EQ(visitor._events[2]._columnName, "id");
    EXPECT_EQ(visitor._events[2]._physicalType, parquet::Type::INT64);

    EXPECT_EQ(visitor._events[3]._kind, RecordedEvent::ColumnEnd);
    EXPECT_EQ(visitor._events[3]._column, 0u);

    EXPECT_EQ(visitor._events[4]._kind, RecordedEvent::ColumnStart);
    EXPECT_EQ(visitor._events[4]._column, 1u);
    EXPECT_EQ(visitor._events[4]._columnName, "name");
    EXPECT_EQ(visitor._events[4]._physicalType, parquet::Type::BYTE_ARRAY);

    EXPECT_EQ(visitor._events[5]._kind, RecordedEvent::ColumnEnd);
    EXPECT_EQ(visitor._events[5]._column, 1u);

    EXPECT_EQ(visitor._events[6]._kind, RecordedEvent::RowGroupEnd);
    EXPECT_EQ(visitor._events[7]._kind, RecordedEvent::FileEnd);

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
    reader.read();

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
    reader.read();

    EXPECT_EQ(visitor._columnStartsSeen, 1);
}

TEST_F(ParquetReaderTest, ThrowsOnMissingFile) {
    RecordingVisitor visitor;
    ParquetReader reader(fs::Path(_outDir + "/does-not-exist.parquet"), visitor);
    EXPECT_THROW(reader.read(), TuringException);
}
