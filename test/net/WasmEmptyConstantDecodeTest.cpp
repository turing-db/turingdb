#include <gtest/gtest.h>

#include <stddef.h>
#include <algorithm>
#include <array>
#include <span>
#include <string_view>
#include <vector>

#include "CellView.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "columns/ColumnConst.h"
#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/PropertyType.h"

#include "ColumnContainer.h"
#include "WasmSink.h"

using namespace db;

namespace {

using Int64 = types::Int64::Primitive;
using String = types::String::Primitive;

constexpr size_t BUFFER_SIZE = 1 << 20;

constexpr size_t ROW_COUNT = 5;

constexpr size_t COLUMN_COUNT = 5;

constexpr std::array<std::string_view, COLUMN_COUNT> NAMES {"i", "s", "l", "m", "c"};

constexpr std::array<std::string_view, 2> SCALAR_NAMES {"i", "s"};

// The columns of a response, decoded as the wasm client's TuringDecoder decodes them
class WasmDecodeSession {
public:
    WasmDecodeSession()
        : _inBuffer(BUFFER_SIZE),
          _decoder(&_inBuffer, &_sink, _schemas)
    {
    }

    void decodeHeader(std::span<const std::string_view> names, std::span<const Column* const> columns) {
        net::proto::TuringProtoOutBuf headerBuffer(BUFFER_SIZE);
        net::proto::TuringProtoEncoder headerEncoder(&headerBuffer);
        headerEncoder.writeColumnHeaders(names, columns);

        load(std::string_view(headerBuffer.data(), headerBuffer.size()));
        _decoder.decodeIncomingChunkHeader(&_container);
    }

    void decodeDataframe(std::span<const Column* const> columns, size_t rowCount) {
        if (rowCount > 0) {
            net::proto::TuringProtoOutBuf outBuffer(BUFFER_SIZE);
            net::proto::TuringProtoEncoder encoder(&outBuffer);
            encoder.writeColumns(columns, 0, rowCount);

            load(std::string_view(outBuffer.data(), outBuffer.size()));
            _decoder.decodeIncomingChunk(&_container);
        }

        const net::proto::WireSize footer = static_cast<net::proto::WireSize>(rowCount);
        load(std::string_view(reinterpret_cast<const char*>(&footer), sizeof(footer)));
        _decoder.decodeChunkFooter(&_container);
    }

    void endChunk() {
        _container.clearColumnData();

        for (net::proto::DecodedColumnSchema& schema : _schemas) {
            net::proto::ProtoColumnState& columnState = schema.getColumnState();
            columnState.reset();
        }

        _decoder.reset();
    }

    wasm::ColumnContainer& getContainer() { return _container; }

private:
    net::proto::TuringProtoInBuf _inBuffer;
    wasm::WasmSink _sink;
    wasm::ColumnContainer _container;
    std::vector<net::proto::DecodedColumnSchema> _schemas;
    net::proto::TuringProtoDecoder<wasm::WasmSink> _decoder;

    void load(std::string_view bytes) {
        _inBuffer.reset();
        std::copy_n(bytes.data(), bytes.size(), _inBuffer.data());
        _inBuffer.increaseWriteOffset(bytes.size());
    }
};

// The wasm client exports column->size() values of a constant column, so a constant of a
// chunk with no rows must hold none for the export never to read its value
void expectNoConstantValue(wasm::ColumnContainer& container, std::span<const std::string_view> names) {
    EXPECT_EQ(container.getRowCount(), 0u);
    ASSERT_EQ(container.size(), names.size());

    for (size_t index = 0; index < names.size(); index++) {
        SCOPED_TRACE(::testing::Message() << "column=" << names[index]);
        const wasm::Column* column = container[index];
        EXPECT_EQ(column->getKind(), wasm::ColumnKind::CONSTANT);
        EXPECT_EQ(column->size(), 0u);
    }
}

}

TEST(WasmEmptyConstantDecodeTest, aConstantOfAChunkWithNoRowHoldsNoValue) {
    const ColumnConst<Int64> integer;
    const ColumnConst<String> string;
    const ColumnConst<ListView> list;
    const ColumnConst<MapView> map;
    const ColumnConst<CellView> cell;
    const std::array<const Column*, COLUMN_COUNT> columns {&integer, &string, &list, &map, &cell};

    WasmDecodeSession session;
    session.decodeHeader(NAMES, columns);
    session.decodeDataframe(columns, 0);

    expectNoConstantValue(session.getContainer(), NAMES);
}

TEST(WasmEmptyConstantDecodeTest, aConstantOfAChunkWithNoRowAfterAChunkWithRowsHoldsNoValue) {
    ColumnConst<Int64> integer;
    integer = Int64 {7};
    ColumnConst<String> string;
    string = String {"a"};
    const std::array<const Column*, 2> columns {&integer, &string};

    WasmDecodeSession session;
    session.decodeHeader(SCALAR_NAMES, columns);

    wasm::ColumnContainer& container = session.getContainer();
    session.decodeDataframe(columns, ROW_COUNT);
    EXPECT_EQ(container.getRowCount(), ROW_COUNT);

    const wasm::WasmSink::ColumnConst<Int64>* decodedInteger = static_cast<const wasm::WasmSink::ColumnConst<Int64>*>(container[0]);
    const wasm::WasmSink::ColumnConst<String>* decodedString = static_cast<const wasm::WasmSink::ColumnConst<String>*>(container[1]);
    ASSERT_EQ(decodedInteger->size(), 1u);
    EXPECT_EQ(decodedInteger->getValue(), 7);
    ASSERT_EQ(decodedString->size(), 1u);
    EXPECT_EQ(decodedString->getValue(), "a");

    session.endChunk();
    session.decodeDataframe(columns, 0);

    expectNoConstantValue(container, SCALAR_NAMES);
}
