#include "TuringTest.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "QueryStatus.h"
#include "SystemManager.h"

#include "FileUtils.h"
#include "Path.h"

#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

void writeNodesParquet(const std::string& path) {
    namespace ps = parquet::schema;

    ps::NodeVector labelListChildren;
    labelListChildren.push_back(ps::PrimitiveNode::Make(
        "element", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8));
    const auto labelList = ps::GroupNode::Make("list", parquet::Repetition::REPEATED, labelListChildren);

    ps::NodeVector labelChildren;
    labelChildren.push_back(labelList);
    const auto labels = ps::GroupNode::Make(
        "__labels", parquet::Repetition::OPTIONAL, labelChildren, parquet::ConvertedType::LIST);

    ps::NodeVector fields;
    fields.push_back(ps::PrimitiveNode::Make("__id", parquet::Repetition::REQUIRED, parquet::Type::INT64));
    fields.push_back(labels);
    const auto schema = std::static_pointer_cast<ps::GroupNode>(
        ps::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);
    const auto out = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    const auto writer = parquet::ParquetFileWriter::Open(out, schema, builder.build());
    parquet::RowGroupWriter* rowGroup = writer->AppendRowGroup();

    const std::vector<int64_t> ids {0, 1, 2, 3};
    auto* idWriter = static_cast<parquet::Int64Writer*>(rowGroup->NextColumn());
    idWriter->WriteBatch(static_cast<int64_t>(ids.size()), nullptr, nullptr, ids.data());

    // One label per node: definition level 3 (present element), repetition level 0
    // (each label starts a new list).
    const std::vector<std::string> labelStrings {"Person", "Employee", "Company", "Manager"};
    std::vector<parquet::ByteArray> labelValues;
    std::vector<int16_t> defLevels;
    std::vector<int16_t> repLevels;
    for (const std::string& label : labelStrings) {
        labelValues.emplace_back(static_cast<uint32_t>(label.size()),
                                 reinterpret_cast<const uint8_t*>(label.data()));
        defLevels.push_back(3);
        repLevels.push_back(0);
    }
    auto* labelWriter = static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn());
    labelWriter->WriteBatch(static_cast<int64_t>(defLevels.size()),
                            defLevels.data(), repLevels.data(), labelValues.data());

    writer->Close();
}

void writeEdgesParquet(const std::string& path) {
    namespace ps = parquet::schema;

    ps::NodeVector fields;
    fields.push_back(ps::PrimitiveNode::Make("__source", parquet::Repetition::REQUIRED, parquet::Type::INT64));
    fields.push_back(ps::PrimitiveNode::Make("__target", parquet::Repetition::REQUIRED, parquet::Type::INT64));
    fields.push_back(ps::PrimitiveNode::Make(
        "__type", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8));
    const auto schema = std::static_pointer_cast<ps::GroupNode>(
        ps::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::UNCOMPRESSED);
    const auto out = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    const auto writer = parquet::ParquetFileWriter::Open(out, schema, builder.build());
    parquet::RowGroupWriter* rowGroup = writer->AppendRowGroup();

    const std::vector<int64_t> sources {0, 1};
    const std::vector<int64_t> targets {1, 2};
    auto* sourceWriter = static_cast<parquet::Int64Writer*>(rowGroup->NextColumn());
    sourceWriter->WriteBatch(static_cast<int64_t>(sources.size()), nullptr, nullptr, sources.data());
    auto* targetWriter = static_cast<parquet::Int64Writer*>(rowGroup->NextColumn());
    targetWriter->WriteBatch(static_cast<int64_t>(targets.size()), nullptr, nullptr, targets.data());

    const std::vector<std::string> typeStrings {"KNOWS", "KNOWS"};
    std::vector<parquet::ByteArray> typeValues;
    for (const std::string& type : typeStrings) {
        typeValues.emplace_back(static_cast<uint32_t>(type.size()),
                                reinterpret_cast<const uint8_t*>(type.data()));
    }
    auto* typeWriter = static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn());
    typeWriter->WriteBatch(static_cast<int64_t>(typeValues.size()), nullptr, nullptr, typeValues.data());

    writer->Close();
}

}

class LoadParquetQueryTest : public TuringTest {
public:
    void initialize() override {
        const auto turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::createSyncedOnDisk(turingDir);
        _db = &_env->getDB();

        const FileUtils::Path dataDir {_env->getConfig().getDataDir().get()};
        const FileUtils::Path importDir = dataDir / "typed";
        FileUtils::createDirectory(importDir);

        writeNodesParquet((importDir / "nodes.parquet").string());
        writeEdgesParquet((importDir / "edges.parquet").string());
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;

    QueryStatus query(std::string_view q, std::string_view graphName = "default") {
        QueryCallbacks callbacks;
        const QueryState state(std::string(graphName), &_env->getMem(), &_queryConfig, &callbacks);
        return _db->query(q, state);
    }
};

TEST_F(LoadParquetQueryTest, loadParquetSucceeds) {
    auto res = query("LOAD PARQUET 'typed' AS typed");
    EXPECT_TRUE(res) << res.getError();

}

TEST_F(LoadParquetQueryTest, canQueryLoadedGraph) {
    auto res = query("LOAD PARQUET 'typed' AS typed");
    ASSERT_TRUE(res) << res.getError();
    EXPECT_TRUE(query("MATCH (n) RETURN n", "typed"));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 10;
    });
}
