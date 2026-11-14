#include <gtest/gtest.h>

#include <stddef.h>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "TuringException.h"
#include "TuringProtoNestedWriter.h"
#include "TuringProtoOutBuf.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"

using namespace db;
using namespace net::proto;

namespace {

constexpr size_t BUFFER_CAPACITY = 256;
constexpr size_t INNER_LIST_LENGTH = 64;
constexpr size_t OUTER_LIST_LENGTH = 4;

using Int64 = types::Int64::Primitive;
using String = types::String::Primitive;
using OptionalCells = std::vector<std::optional<CellView>>;

enum class WriteEntry {
    ListView,
    MapView,
    Cells,
    OptionalCells,
    MapEntries,
};

std::string_view toString(WriteEntry entry) {
    switch (entry) {
        case WriteEntry::ListView:
            return "ListView";
        break;
        case WriteEntry::MapView:
            return "MapView";
        break;
        case WriteEntry::Cells:
            return "Cells";
        break;
        case WriteEntry::OptionalCells:
            return "OptionalCells";
        break;
        case WriteEntry::MapEntries:
            return "MapEntries";
        break;
    }

    return "";
}

void throwOnFull(TuringProtoOutBuf& outBuf) {
    outBuf.setOnBufferFullCallBack([]() {
        throw TuringException("The buffer is full");
    });
}

}

// A write that throws part way through a nested container must not leave anything behind in
// the writer: the next write, on the same writer, writes the bytes a fresh writer writes
class NestedWriterAfterThrowTest : public ::testing::TestWithParam<std::tuple<WriteEntry, WriteEntry>> {
protected:
    void SetUp() override {
        std::vector<ListBuffer<>::ListItemVariant> innerElements;
        for (size_t index = 0; index < INNER_LIST_LENGTH; index++) {
            innerElements.emplace_back(Int64 {static_cast<int64_t>(1'000'000 + index)});
        }
        const ListView tooLargeInner = _listBuffer.insert(innerElements);

        const std::vector<ListBuffer<>::ListItemVariant> outerElements(OUTER_LIST_LENGTH, tooLargeInner);
        _tooLargeList = _listBuffer.insert(outerElements);

        const std::vector<MapBuffer<>::MapKeyValuePair> tooLargeEntries = {{"a", _tooLargeList}, {"b", _tooLargeList}};
        _tooLargeMap = _mapBuffer.insert(tooLargeEntries);

        for (const CellView& element : _tooLargeList.elements()) {
            _tooLargeOptionalCells.emplace_back(element);
        }

        const std::vector<ListBuffer<>::ListItemVariant> smallInnerElements = {Int64 {2}, Int64 {3}};
        const ListView smallInner = _listBuffer.insert(smallInnerElements);

        const std::vector<ListBuffer<>::ListItemVariant> smallElements = {Int64 {1}, smallInner, String {"text"}};
        _smallList = _listBuffer.insert(smallElements);

        const std::vector<MapBuffer<>::MapKeyValuePair> smallInnerEntries = {{"x", String {"y"}}};
        const MapView smallInnerMap = _mapBuffer.insert(smallInnerEntries);

        const std::vector<MapBuffer<>::MapKeyValuePair> smallEntries = {{"k", _smallList}, {"m", smallInnerMap}};
        _smallMap = _mapBuffer.insert(smallEntries);

        for (const CellView& element : _smallList.elements()) {
            _smallOptionalCells.emplace_back(element);
        }
        _smallOptionalCells.emplace_back(std::nullopt);
    }

    static void write(WriteEntry entry,
                      NestedContainerWriter& writer,
                      const ListView& list,
                      const MapView& map,
                      const OptionalCells& optionalCells) {
        switch (entry) {
            case WriteEntry::ListView:
                writer.writeListView(list);
            break;
            case WriteEntry::MapView:
                writer.writeMapView(map);
            break;
            case WriteEntry::Cells:
                writer.writeCells(list.elements());
            break;
            case WriteEntry::OptionalCells:
                writer.writeOptionalCells(optionalCells);
            break;
            case WriteEntry::MapEntries:
                writer.writeMapEntries(map.entries());
            break;
        }
    }

    void writeTooLarge(WriteEntry entry, NestedContainerWriter& writer) {
        write(entry, writer, _tooLargeList, _tooLargeMap, _tooLargeOptionalCells);
    }

    void writeSmall(WriteEntry entry, NestedContainerWriter& writer) {
        write(entry, writer, _smallList, _smallMap, _smallOptionalCells);
    }

    ListBuffer<> _listBuffer;
    MapBuffer<> _mapBuffer;

    ListView _tooLargeList;
    MapView _tooLargeMap;
    OptionalCells _tooLargeOptionalCells;

    ListView _smallList;
    MapView _smallMap;
    OptionalCells _smallOptionalCells;
};

TEST_P(NestedWriterAfterThrowTest, aWriteAfterAThrowWritesWhatAFreshWriterWrites) {
    const auto [failingEntry, nextEntry] = GetParam();

    TuringProtoOutBuf freshBuf(BUFFER_CAPACITY);
    throwOnFull(freshBuf);
    NestedContainerWriter freshWriter(&freshBuf);
    writeSmall(nextEntry, freshWriter);
    const std::string expected(freshBuf.data(), freshBuf.size());

    TuringProtoOutBuf outBuf(BUFFER_CAPACITY);
    throwOnFull(outBuf);
    NestedContainerWriter writer(&outBuf);
    EXPECT_THROW(writeTooLarge(failingEntry, writer), TuringException);

    outBuf.reset();
    ASSERT_NO_THROW(writeSmall(nextEntry, writer));

    const std::string written(outBuf.data(), outBuf.size());
    EXPECT_EQ(written, expected);
}

TEST_P(NestedWriterAfterThrowTest, aTooLargeWriteAfterAThrowStillThrows) {
    const auto [failingEntry, nextEntry] = GetParam();

    TuringProtoOutBuf outBuf(BUFFER_CAPACITY);
    throwOnFull(outBuf);
    NestedContainerWriter writer(&outBuf);
    EXPECT_THROW(writeTooLarge(failingEntry, writer), TuringException);

    outBuf.reset();
    EXPECT_THROW(writeTooLarge(nextEntry, writer), TuringException);

    outBuf.reset();
    ASSERT_NO_THROW(writeSmall(nextEntry, writer));

    TuringProtoOutBuf freshBuf(BUFFER_CAPACITY);
    throwOnFull(freshBuf);
    NestedContainerWriter freshWriter(&freshBuf);
    writeSmall(nextEntry, freshWriter);

    EXPECT_EQ(std::string(outBuf.data(), outBuf.size()), std::string(freshBuf.data(), freshBuf.size()));
}

INSTANTIATE_TEST_SUITE_P(EveryEntryPoint,
                         NestedWriterAfterThrowTest,
                         ::testing::Combine(::testing::Values(WriteEntry::ListView,
                                                              WriteEntry::MapView,
                                                              WriteEntry::Cells,
                                                              WriteEntry::OptionalCells,
                                                              WriteEntry::MapEntries),
                                            ::testing::Values(WriteEntry::ListView,
                                                              WriteEntry::MapView,
                                                              WriteEntry::Cells,
                                                              WriteEntry::OptionalCells,
                                                              WriteEntry::MapEntries)),
                         [](const ::testing::TestParamInfo<std::tuple<WriteEntry, WriteEntry>>& info) {
                             return std::string(toString(std::get<0>(info.param))) + "Then"
                                  + std::string(toString(std::get<1>(info.param)));
                         });
