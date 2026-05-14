#include "TuringTest.h"

#include "ParquetReader.h"

#include <arrow/io/file.h>
#include <parquet/column_reader.h>
#include <parquet/column_writer.h>
#include <parquet/file_reader.h>
#include <parquet/file_writer.h>
#include <parquet/metadata.h>
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
        ColumnChunk,
        RowGroupEnd,
        FileEnd,
    };

    Kind _kind {FileStart};
    int _rowGroup {-1};
    int _column {-1};
    int64_t _numRows {0};
    std::string _columnName;
    parquet::Type::type _physicalType {parquet::Type::BOOLEAN};
};

class RecordingVisitor : public ParquetSaxVisitor {
public:
    std::vector<RecordedEvent> _events;
    std::vector<int64_t> _idValues;
    std::vector<std::string> _nameValues;

    bool onFileStart(const parquet::FileMetaData& /*metadata*/) override {
        _events.push_back({RecordedEvent::FileStart, -1, -1, 0, "", parquet::Type::BOOLEAN});
        return true;
    }

    bool onRowGroupStart(int idx, const parquet::RowGroupMetaData& metadata) override {
        _events.push_back({RecordedEvent::RowGroupStart, idx, -1,
                           metadata.num_rows(), "", parquet::Type::BOOLEAN});
        return true;
    }

    bool onColumnChunk(int rg,
                       int col,
                       const parquet::ColumnDescriptor& descriptor,
                       parquet::ColumnReader& reader) override {
        RecordedEvent e;
        e._kind = RecordedEvent::ColumnChunk;
        e._rowGroup = rg;
        e._column = col;
        e._columnName = descriptor.name();
        e._physicalType = descriptor.physical_type();
        _events.push_back(e);

        if (descriptor.physical_type() == parquet::Type::INT64) {
            auto* typed = static_cast<parquet::Int64Reader*>(&reader);
            constexpr int64_t kBatch = 1024;
            int64_t buf[kBatch];
            int64_t valuesRead = 0;
            while (typed->HasNext()) {
                typed->ReadBatch(kBatch, nullptr, nullptr, buf, &valuesRead);
                for (int64_t i = 0; i < valuesRead; ++i) {
                    _idValues.push_back(buf[i]);
                }
            }
        } else if (descriptor.physical_type() == parquet::Type::BYTE_ARRAY) {
            auto* typed = static_cast<parquet::ByteArrayReader*>(&reader);
            constexpr int64_t kBatch = 1024;
            parquet::ByteArray buf[kBatch];
            int64_t valuesRead = 0;
            while (typed->HasNext()) {
                typed->ReadBatch(kBatch, nullptr, nullptr, buf, &valuesRead);
                for (int64_t i = 0; i < valuesRead; ++i) {
                    _nameValues.emplace_back(reinterpret_cast<const char*>(buf[i].ptr),
                                             buf[i].len);
                }
            }
        }

        return true;
    }

    bool onRowGroupEnd(int idx) override {
        _events.push_back({RecordedEvent::RowGroupEnd, idx, -1, 0, "", parquet::Type::BOOLEAN});
        return true;
    }

    bool onFileEnd() override {
        _events.push_back({RecordedEvent::FileEnd, -1, -1, 0, "", parquet::Type::BOOLEAN});
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

    ASSERT_EQ(visitor._events.size(), 6u);

    EXPECT_EQ(visitor._events[0]._kind, RecordedEvent::FileStart);

    EXPECT_EQ(visitor._events[1]._kind, RecordedEvent::RowGroupStart);
    EXPECT_EQ(visitor._events[1]._rowGroup, 0);
    EXPECT_EQ(visitor._events[1]._numRows, 3);

    EXPECT_EQ(visitor._events[2]._kind, RecordedEvent::ColumnChunk);
    EXPECT_EQ(visitor._events[2]._column, 0);
    EXPECT_EQ(visitor._events[2]._columnName, "id");
    EXPECT_EQ(visitor._events[2]._physicalType, parquet::Type::INT64);

    EXPECT_EQ(visitor._events[3]._kind, RecordedEvent::ColumnChunk);
    EXPECT_EQ(visitor._events[3]._column, 1);
    EXPECT_EQ(visitor._events[3]._columnName, "name");
    EXPECT_EQ(visitor._events[3]._physicalType, parquet::Type::BYTE_ARRAY);

    EXPECT_EQ(visitor._events[4]._kind, RecordedEvent::RowGroupEnd);
    EXPECT_EQ(visitor._events[5]._kind, RecordedEvent::FileEnd);

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

    size_t columnChunkEvents = 0;
    for (const auto& e : visitor._events) {
        if (e._kind == RecordedEvent::ColumnChunk) {
            ++columnChunkEvents;
        }
    }

    EXPECT_EQ(columnChunkEvents, 1u);
    EXPECT_EQ(visitor._idValues, ids);
    EXPECT_TRUE(visitor._nameValues.empty());
}

class AbortingVisitor : public ParquetSaxVisitor {
public:
    int _columnChunksSeen {0};

    bool onColumnChunk(int /*rg*/,
                       int /*col*/,
                       const parquet::ColumnDescriptor& /*descriptor*/,
                       parquet::ColumnReader& /*reader*/) override {
        ++_columnChunksSeen;
        return false;
    }
};

TEST_F(ParquetReaderTest, ReturningFalseAbortsRead) {
    writeTestParquet({1, 2, 3}, {"a", "b", "c"});

    AbortingVisitor visitor;
    ParquetReader reader(fs::Path(_parquetPath), visitor);
    reader.read();

    EXPECT_EQ(visitor._columnChunksSeen, 1);
}

TEST_F(ParquetReaderTest, ThrowsOnMissingFile) {
    RecordingVisitor visitor;
    ParquetReader reader(fs::Path(_outDir + "/does-not-exist.parquet"), visitor);
    EXPECT_THROW(reader.read(), TuringException);
}
