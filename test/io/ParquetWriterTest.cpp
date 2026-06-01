#include "TuringTest.h"

#include "ParquetReader.h"
#include "ParquetWriter.h"
#include "ParquetWriteSchema.h"

#include <stdint.h>

#include <array>
#include <limits>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>
#include <parquet/types.h>

#include "Path.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects decoded values per column index across all chunks/row groups so a
// written file can be compared against the inputs.
class CollectingVisitor : public ParquetSaxVisitor {
public:
    std::map<size_t, std::vector<int64_t>> _int64;
    std::map<size_t, std::vector<double>> _doubles;
    std::map<size_t, std::vector<bool>> _bools;
    std::map<size_t, std::vector<std::string>> _strings;
    std::map<size_t, std::vector<float>> _flbaFloats;
    std::map<size_t, size_t> _flbaByteWidth;
    std::vector<std::pair<std::string, std::string>> _metadata;

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        if (keyValueMetadata) {
            for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
                _metadata.emplace_back(keyValueMetadata->key(i), keyValueMetadata->value(i));
            }
        }
        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = _int64[columnIndex];
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

    bool onDoubleValues(size_t columnIndex, std::span<const double> values) override {
        std::vector<double>& target = _doubles[columnIndex];
        for (const double value : values) {
            target.push_back(value);
        }
        return true;
    }

    bool onBoolValues(size_t columnIndex, std::span<const bool> values) override {
        std::vector<bool>& target = _bools[columnIndex];
        for (const bool value : values) {
            target.push_back(value);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        std::vector<std::string>& target = _strings[columnIndex];
        for (const auto& byteArray : values) {
            target.emplace_back(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }

    bool onFixedLenByteArrayValues(size_t columnIndex,
                                   std::span<const parquet::FixedLenByteArray> values,
                                   size_t byteWidth) override {
        _flbaByteWidth[columnIndex] = byteWidth;
        std::vector<float>& target = _flbaFloats[columnIndex];
        const size_t floatsPerValue = byteWidth / sizeof(float);
        for (const auto& value : values) {
            const float* floats = reinterpret_cast<const float*>(value.ptr);
            for (size_t i = 0; i < floatsPerValue; ++i) {
                target.push_back(floats[i]);
            }
        }
        return true;
    }
};

void readBack(const std::string& path, CollectingVisitor& visitor) {
    ParquetReader reader(fs::Path(path), visitor);
    while (reader.nextChunk()) {
    }
}

}

class ParquetWriterTest : public TuringTest {
protected:
    std::string _parquetPath;

    void initialize() override {
        _parquetPath = _outDir + "/writer-test.parquet";
    }
};

TEST_F(ParquetWriterTest, AllColumnTypesRoundTrip) {
    const std::vector<int64_t> ids {10, 20, 30, 40};
    const std::vector<uint64_t> bigs {
        0u,
        1u,
        (static_cast<uint64_t>(1) << 63),
        std::numeric_limits<uint64_t>::max()};
    const std::vector<double> scores {1.5, 2.5, -3.5, 0.0};
    const std::array<bool, 4> flags {true, false, true, false};
    const std::vector<std::string> names {"alice", "bob", "", "carol-longer-name"};
    constexpr size_t dimension = 3;
    const std::vector<float> vectors {
        1.0f, 2.0f, 3.0f,
        4.5f, 5.5f, 6.5f,
        -1.0f, 0.0f, 100.25f,
        7.0f, 8.0f, 9.0f};

    std::vector<int64_t> bigsAsInt64;
    bigsAsInt64.reserve(bigs.size());
    for (const uint64_t value : bigs) {
        bigsAsInt64.push_back(static_cast<int64_t>(value));
    }

    std::vector<std::string_view> nameViews;
    nameViews.reserve(names.size());
    for (const std::string& name : names) {
        nameViews.emplace_back(name);
    }

    {
        ParquetWriteSchema schema;
        schema.addColumn("id", ParquetColumnType::Int64);
        schema.addColumn("big", ParquetColumnType::UInt64);
        schema.addColumn("score", ParquetColumnType::Double);
        schema.addColumn("flag", ParquetColumnType::Bool);
        schema.addColumn("name", ParquetColumnType::String);
        schema.addFixedLenColumn("vec", dimension * sizeof(float));

        ParquetWriter writer(fs::Path(_parquetPath), schema);
        writer.setMetadata("dimension", "3");
        writer.setMetadata("producer", "turingdb-test");
        writer.beginRowGroup(ids.size());
        writer.writeInt64Column(0, ids);
        writer.writeInt64Column(1, bigsAsInt64);
        writer.writeDoubleColumn(2, scores);
        writer.writeBoolColumn(3, std::span<const bool>(flags.data(), flags.size()));
        writer.writeStringColumn(4, nameViews);
        writer.writeFixedLenColumn(5,
                                   std::span<const float>(vectors.data(), vectors.size()),
                                   dimension * sizeof(float));
        writer.finish();
    }

    CollectingVisitor visitor;
    readBack(_parquetPath, visitor);

    EXPECT_EQ(visitor._int64[0], ids);

    ASSERT_EQ(visitor._int64[1].size(), bigs.size());
    for (size_t i = 0; i < bigs.size(); ++i) {
        EXPECT_EQ(static_cast<uint64_t>(visitor._int64[1][i]), bigs[i]);
    }

    EXPECT_EQ(visitor._doubles[2], scores);

    ASSERT_EQ(visitor._bools[3].size(), flags.size());
    for (size_t i = 0; i < flags.size(); ++i) {
        EXPECT_EQ(visitor._bools[3][i], flags[i]);
    }

    EXPECT_EQ(visitor._strings[4], names);

    EXPECT_EQ(visitor._flbaByteWidth[5], dimension * sizeof(float));
    EXPECT_EQ(visitor._flbaFloats[5], vectors);

    bool sawDimension = false;
    bool sawProducer = false;
    for (const auto& [key, value] : visitor._metadata) {
        if (key == "dimension") {
            sawDimension = (value == "3");
        } else if (key == "producer") {
            sawProducer = (value == "turingdb-test");
        }
    }
    EXPECT_TRUE(sawDimension);
    EXPECT_TRUE(sawProducer);
}

TEST_F(ParquetWriterTest, MultipleRowGroupsPreserveOrder) {
    const std::vector<int64_t> first {1, 2, 3};
    const std::vector<int64_t> second {4, 5};

    {
        ParquetWriteSchema schema;
        schema.addColumn("id", ParquetColumnType::Int64);

        ParquetWriter writer(fs::Path(_parquetPath), schema);
        writer.beginRowGroup(first.size());
        writer.writeInt64Column(0, first);
        writer.beginRowGroup(second.size());
        writer.writeInt64Column(0, second);
        writer.finish();
    }

    CollectingVisitor visitor;
    readBack(_parquetPath, visitor);

    const std::vector<int64_t> expected {1, 2, 3, 4, 5};
    EXPECT_EQ(visitor._int64[0], expected);
}
