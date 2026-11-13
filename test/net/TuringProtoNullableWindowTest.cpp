#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "TuringProtoEncoder.h"
#include "TuringProtoOutBuf.h"
#include "LocalMemory.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "metadata/PropertyType.h"

#include "FatalException.h"

using namespace db;

namespace {

using UInt64 = types::UInt64::Primitive;
using Int64 = types::Int64::Primitive;
using Double = types::Double::Primitive;
using StringView = types::String::Primitive;
using Bool = types::Bool::Primitive;
using Embedding = types::Embedding::Primitive;
using DateTime = types::DateTime::Primitive;
using Duration = types::Duration::Primitive;
using Point = types::Point::Primitive;

constexpr size_t BUFFER_SIZE = 1 << 16;

std::string encodeRows(const Column* column, size_t offset, size_t rowCount) {
    net::proto::TuringProtoOutBuf outBuf(BUFFER_SIZE);
    outBuf.setOnBufferFullCallBack([]() { throw std::length_error("The encoded rows outgrew the buffer"); });

    net::proto::TuringProtoEncoder encoder(&outBuf);
    const std::array<const Column*, 1> columns {column};
    encoder.writeColumns(columns, offset, rowCount);

    return std::string(outBuf.data(), outBuf.size());
}

template <typename T>
void fillColumn(ColumnOptVector<T>& column, std::span<const std::optional<T>> rows) {
    for (const std::optional<T>& row : rows) {
        if (row.has_value()) {
            column.push_back(*row);
        } else {
            column.pushNull();
        }
    }
}

template <typename T>
void expectWindowEncodesLikeItsRows(const ColumnOptVector<T>& full,
                                    const std::vector<std::optional<T>>& rows,
                                    size_t offset,
                                    size_t rowCount) {
    SCOPED_TRACE(::testing::Message() << "offset=" << offset << " rowCount=" << rowCount);

    const size_t windowRowCount = std::min(rowCount, rows.size() - offset);

    ColumnOptVector<T> window;
    fillColumn<T>(window, std::span<const std::optional<T>>(rows).subspan(offset, windowRowCount));

    EXPECT_EQ(encodeRows(&full, offset, rowCount), encodeRows(&window, 0, windowRowCount));
}

template <typename T>
void expectWindowsEncodeLikeTheirRows(const std::vector<std::optional<T>>& rows) {
    ColumnOptVector<T> full;
    fillColumn<T>(full, rows);

    for (size_t offset = 0; offset < rows.size(); offset++) {
        for (size_t rowCount = 1; offset + rowCount <= rows.size(); rowCount++) {
            expectWindowEncodesLikeItsRows<T>(full, rows, offset, rowCount);
        }
    }
}

}

TEST(TuringProtoNullableWindowTest, EncodesTheWindowOfNullableScalarColumns) {
    expectWindowsEncodeLikeTheirRows<UInt64>({1, std::nullopt, 3, std::nullopt, 5});
    expectWindowsEncodeLikeTheirRows<Int64>({-1, std::nullopt, 3, 4, std::nullopt});
    expectWindowsEncodeLikeTheirRows<Double>({std::nullopt, 1.5, std::nullopt, -2.25, 8.0});
    expectWindowsEncodeLikeTheirRows<Bool>({Bool {true}, std::nullopt, Bool {false}, std::nullopt, Bool {true}});
    expectWindowsEncodeLikeTheirRows<DateTime>({DateTime {7}, std::nullopt, DateTime {-9}, DateTime {11}, std::nullopt});
    expectWindowsEncodeLikeTheirRows<Duration>({std::nullopt, Duration {60}, std::nullopt, Duration {-1}, Duration {2}});
    expectWindowsEncodeLikeTheirRows<Point>({Point(PointCRS::Cartesian, 1.5, -2.0, 0.0),
                                             std::nullopt,
                                             Point(PointCRS::WGS84_3D, 2.3522, 48.8566, 35.0),
                                             std::nullopt,
                                             Point(PointCRS::Cartesian3D, 1.0, 2.0, 3.0)});
    expectWindowsEncodeLikeTheirRows<NodeID>({NodeID {4}, std::nullopt, NodeID {0}, std::nullopt, NodeID {9}});
}

TEST(TuringProtoNullableWindowTest, EncodesWindowsAcrossNullMaskWords) {
    using namespace std::string_literals;

    std::vector<std::optional<Int64>> integers;
    std::vector<std::optional<std::string>> strings;
    for (size_t row = 0; row < 130; row++) {
        const bool isNull = row % 3 == 1;
        integers.push_back(isNull ? std::nullopt : std::optional<Int64> {static_cast<Int64>(row)});
        strings.push_back(isNull ? std::nullopt : std::optional<std::string> {std::to_string(row) + "s"s});
    }

    ColumnOptVector<Int64> fullIntegers;
    fillColumn<Int64>(fullIntegers, integers);

    ColumnOptVector<std::string> fullStrings;
    fillColumn<std::string>(fullStrings, strings);

    const std::vector<std::pair<size_t, size_t>> windows {
        {0, 130}, {60, 10}, {63, 2}, {64, 64}, {1, 128}, {63, 67}, {64, 66}, {120, 10}, {129, 1}, {100, 64},
    };

    for (const auto& [offset, rowCount] : windows) {
        expectWindowEncodesLikeItsRows<Int64>(fullIntegers, integers, offset, rowCount);
        expectWindowEncodesLikeItsRows<std::string>(fullStrings, strings, offset, rowCount);
    }
}

TEST(TuringProtoNullableWindowTest, EncodesTheWindowOfNullableStringAndEmbeddingColumns) {
    using namespace std::string_literals;
    using namespace std::string_view_literals;

    expectWindowsEncodeLikeTheirRows<StringView>({"first"sv, std::nullopt, ""sv, "fourth"sv, std::nullopt});
    expectWindowsEncodeLikeTheirRows<std::string>({std::nullopt, "owned"s, std::nullopt, "strings"s, ""s});

    const std::vector<float> first {1.0f, 2.0f, 3.0f};
    const std::vector<float> last {-0.5f};
    expectWindowsEncodeLikeTheirRows<Embedding>({Embedding {first}, std::nullopt, Embedding {}, std::nullopt, Embedding {last}});
}

TEST(TuringProtoNullableWindowTest, EncodesTheWindowOfNullableListMapAndCellColumns) {
    using MapEntry = MapBuffer<>::MapKeyValuePair;
    using namespace std::string_view_literals;

    LocalMemory localMemory;

    std::vector<ListBuffer<>::ListItemVariant> firstItems;
    firstItems.emplace_back(Int64 {1});
    firstItems.emplace_back(StringView {"two"sv});
    const ListView firstList = localMemory.listBuffer().insert(firstItems);

    std::vector<ListBuffer<>::ListItemVariant> lastItems;
    lastItems.emplace_back(Int64 {7});
    const ListView lastList = localMemory.listBuffer().insert(lastItems);

    const std::vector<ListBuffer<>::ListItemVariant> noItems;
    const ListView emptyList = localMemory.listBuffer().insert(noItems);

    expectWindowsEncodeLikeTheirRows<ListView>({firstList, std::nullopt, emptyList, lastList, std::nullopt});

    const std::vector<MapEntry> firstEntries = {{"a", Int64 {1}}, {"b", StringView {"bee"sv}}};
    const MapView firstMap = localMemory.mapBuffer().insert(firstEntries);

    const std::vector<MapEntry> lastEntries = {{"z", Int64 {26}}};
    const MapView lastMap = localMemory.mapBuffer().insert(lastEntries);

    expectWindowsEncodeLikeTheirRows<MapView>({std::nullopt, firstMap, std::nullopt, lastMap, firstMap});

    std::vector<CellView> cells;
    for (const CellView cell : firstList) {
        cells.push_back(cell);
    }

    expectWindowsEncodeLikeTheirRows<CellView>({cells[0], std::nullopt, cells[1], std::nullopt, cells[0]});
}

TEST(TuringProtoNullableWindowTest, RejectsAnEmptyNullableConstant) {
    const ColumnConst<std::optional<Int64>> empty;

    EXPECT_THROW(encodeRows(&empty, 0, 1), FatalException);
}
