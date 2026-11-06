#include "ParquetPointImportTest.h"

#include <string.h>

#include <memory>

#include <arrow/io/file.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

#include "FileUtils.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr uint32_t EWKB_POINT_WITH_SRID = 0x20000001;
constexpr uint32_t EWKB_Z_FLAG = 0x80000000;

template <typename Value>
void appendValue(std::string& bytes, Value value) {
    char raw[sizeof(Value)];
    memcpy(raw, &value, sizeof(Value));
    bytes.append(raw, sizeof(Value));
}

}

std::string turing::test::wkbPoint(double x, double y, std::optional<double> z) {
    std::string bytes;
    appendValue<uint8_t>(bytes, 1);
    appendValue<uint32_t>(bytes, z ? 1001 : 1);
    appendValue(bytes, x);
    appendValue(bytes, y);

    if (z) {
        appendValue(bytes, *z);
    }

    return bytes;
}

std::string turing::test::wkbOfType(uint32_t type, std::initializer_list<double> values) {
    std::string bytes;
    appendValue<uint8_t>(bytes, 1);
    appendValue<uint32_t>(bytes, type);

    for (const double value : values) {
        appendValue(bytes, value);
    }

    return bytes;
}

std::string turing::test::bigEndianEWKBPoint(double x, double y) {
    std::string bytes;
    bytes.push_back(0);

    const auto appendBigEndian = [&bytes]<typename Value>(Value value) {
        char raw[sizeof(Value)];
        memcpy(raw, &value, sizeof(Value));
        for (size_t index = sizeof(Value); index > 0; index--) {
            bytes.push_back(raw[index - 1]);
        }
    };

    appendBigEndian(EWKB_POINT_WITH_SRID);
    appendBigEndian(uint32_t {4326});
    appendBigEndian(x);
    appendBigEndian(y);

    return bytes;
}

std::string turing::test::ewkbPoint(double x, double y, uint32_t srid, std::optional<double> z) {
    std::string bytes;
    appendValue<uint8_t>(bytes, 1);
    appendValue<uint32_t>(bytes, z ? EWKB_POINT_WITH_SRID | EWKB_Z_FLAG : EWKB_POINT_WITH_SRID);
    appendValue<uint32_t>(bytes, srid);
    appendValue(bytes, x);
    appendValue(bytes, y);

    if (z) {
        appendValue(bytes, *z);
    }

    return bytes;
}

void ParquetPointImportTest::writeImport(std::string_view directory,
                                         GeometryMarking marking,
                                         std::string_view crs,
                                         const std::vector<std::optional<std::string>>& points,
                                         const std::vector<std::vector<std::string>>& routes) {
    FileUtils::Path dataDir;
    getDataDir(dataDir);

    const FileUtils::Path importDir = dataDir / std::string {directory};
    FileUtils::createDirectory(importDir);

    const FileUtils::Path fixtureDir {PARQUET_TEST_DATA_DIR};
    FileUtils::copy(fixtureDir / "minimal_edges.parquet", importDir / "edges.parquet");

    namespace schema = parquet::schema;

    const std::string crsText {crs};
    std::shared_ptr<const parquet::LogicalType> atType = parquet::LogicalType::None();
    std::shared_ptr<const arrow::KeyValueMetadata> keyValues;

    if (marking == GeometryMarking::GeometryLogicalType) {
        atType = parquet::LogicalType::Geometry(crsText);
    } else if (marking == GeometryMarking::GeographyLogicalType) {
        atType = parquet::LogicalType::Geography(crsText);
    } else {
        keyValues = arrow::key_value_metadata({"geo"}, {crsText});
    }

    const schema::NodePtr element = schema::PrimitiveNode::Make("element",
                                                                parquet::Repetition::OPTIONAL,
                                                                parquet::LogicalType::String(),
                                                                parquet::Type::BYTE_ARRAY);
    const schema::NodePtr list = schema::GroupNode::Make("list", parquet::Repetition::REPEATED, {element});

    schema::NodeVector fields = {
        schema::PrimitiveNode::Make("__id", parquet::Repetition::REQUIRED, parquet::Type::INT64),
        schema::GroupNode::Make("__labels", parquet::Repetition::OPTIONAL, {list}, parquet::LogicalType::List()),
        schema::PrimitiveNode::Make("at", parquet::Repetition::OPTIONAL, atType, parquet::Type::BYTE_ARRAY),
    };

    if (!routes.empty()) {
        const schema::NodePtr stop = schema::PrimitiveNode::Make("element",
                                                                 parquet::Repetition::OPTIONAL,
                                                                 atType,
                                                                 parquet::Type::BYTE_ARRAY);
        const schema::NodePtr stops = schema::GroupNode::Make("list", parquet::Repetition::REPEATED, {stop});
        fields.push_back(schema::GroupNode::Make("route", parquet::Repetition::OPTIONAL, {stops}, parquet::LogicalType::List()));
    }

    const auto root = std::static_pointer_cast<schema::GroupNode>(
        schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    const std::string nodePath = (importDir / "nodes.parquet").string();
    auto file = arrow::io::FileOutputStream::Open(nodePath).ValueOrDie();
    std::unique_ptr<parquet::ParquetFileWriter> writer =
        parquet::ParquetFileWriter::Open(file, root, parquet::default_writer_properties(), keyValues);

    parquet::RowGroupWriter* rowGroup = writer->AppendRowGroup();
    const int64_t rowCount = static_cast<int64_t>(points.size());

    std::vector<int64_t> ids(points.size());
    for (size_t index = 0; index < points.size(); index++) {
        ids[index] = static_cast<int64_t>(index);
    }

    static_cast<parquet::Int64Writer*>(rowGroup->NextColumn())->WriteBatch(rowCount, nullptr, nullptr, ids.data());

    parquet::ByteArrayWriter* labelWriter = static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn());
    const int16_t labelDefLevel = labelWriter->descr()->max_definition_level();
    const std::vector<int16_t> labelDefLevels(points.size(), labelDefLevel);
    const std::vector<int16_t> labelRepLevels(points.size(), 0);
    const std::vector<parquet::ByteArray> labels(points.size(), parquet::ByteArray("City"));
    labelWriter->WriteBatch(rowCount, labelDefLevels.data(), labelRepLevels.data(), labels.data());

    std::vector<int16_t> atDefLevels;
    std::vector<parquet::ByteArray> atValues;
    for (const std::optional<std::string>& point : points) {
        atDefLevels.push_back(point ? 1 : 0);
        if (point) {
            atValues.emplace_back(static_cast<uint32_t>(point->size()),
                                  reinterpret_cast<const uint8_t*>(point->data()));
        }
    }

    static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn())->WriteBatch(rowCount,
                                                                               atDefLevels.data(),
                                                                               nullptr,
                                                                               atValues.data());

    if (!routes.empty()) {
        parquet::ByteArrayWriter* routeWriter = static_cast<parquet::ByteArrayWriter*>(rowGroup->NextColumn());
        const int16_t stopDefLevel = routeWriter->descr()->max_definition_level();

        std::vector<int16_t> routeDefLevels;
        std::vector<int16_t> routeRepLevels;
        std::vector<parquet::ByteArray> routeValues;

        for (const std::vector<std::string>& route : routes) {
            if (route.empty()) {
                routeDefLevels.push_back(stopDefLevel - 2);
                routeRepLevels.push_back(0);
            }

            for (size_t index = 0; index < route.size(); index++) {
                routeDefLevels.push_back(stopDefLevel);
                routeRepLevels.push_back(index == 0 ? 0 : 1);
                routeValues.emplace_back(static_cast<uint32_t>(route[index].size()),
                                         reinterpret_cast<const uint8_t*>(route[index].data()));
            }
        }

        routeWriter->WriteBatch(static_cast<int64_t>(routeDefLevels.size()),
                                routeDefLevels.data(),
                                routeRepLevels.data(),
                                routeValues.data());
    }

    writer->Close();
}
