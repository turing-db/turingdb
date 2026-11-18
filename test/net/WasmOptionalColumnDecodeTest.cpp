#include <gtest/gtest.h>

#include <stdint.h>
#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "TuringProtoDecoder.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"

#include "ColumnContainer.h"
#include "WasmSink.h"

using namespace db;

namespace {

using Int64 = types::Int64::Primitive;
using Double = types::Double::Primitive;
using Bool = types::Bool::Primitive;
using String = types::String::Primitive;

constexpr size_t LARGE_BUFFER_SIZE = 1 << 20;

constexpr std::array<size_t, 8> ROW_COUNTS {1, 7, 9, 63, 64, 65, 130, 200};

constexpr std::array<size_t, 3> BUFFER_SIZES {24, 97, LARGE_BUFFER_SIZE};

const std::array<std::string, 4> STRINGS {"", "a", "turing", "a longer string value"};

template <typename T>
T makeValue(size_t row);

template <>
Int64 makeValue<Int64>(size_t row) {
    return static_cast<Int64>(row * 7919) - 1000;
}

template <>
Double makeValue<Double>(size_t row) {
    return static_cast<Double>(row) * 1.25 - 3.5;
}

template <>
Bool makeValue<Bool>(size_t row) {
    return Bool {row % 2 == 0};
}

template <>
String makeValue<String>(size_t row) {
    return STRINGS[row % STRINGS.size()];
}

template <typename T>
void makeRows(size_t rowCount, size_t nullEvery, std::vector<std::optional<T>>& rows) {
    for (size_t row = 0; row < rowCount; row++) {
        const bool isNull = nullEvery != 0 && (row % nullEvery == 1 || row % 64 == 63);
        if (isNull) {
            rows.push_back(std::nullopt);
        } else {
            rows.push_back(makeValue<T>(row));
        }
    }
}

template <typename T>
void fillColumn(ColumnOptVector<T>* column, const std::vector<std::optional<T>>& rows) {
    for (const std::optional<T>& row : rows) {
        if (row.has_value()) {
            column->push_back(*row);
        } else {
            column->pushNull();
        }
    }
}

void encodeChunks(const Column* column, size_t rowCount, size_t bufferSize, std::vector<std::string>& chunks) {
    net::proto::TuringProtoOutBuf outBuf(bufferSize);
    outBuf.setOnBufferFullCallBack([&outBuf, &chunks]() {
        chunks.emplace_back(outBuf.data(), outBuf.size());
        outBuf.reset();
    });

    net::proto::TuringProtoEncoder encoder(&outBuf);
    const std::array<const Column*, 1> columns {column};
    encoder.writeColumns(columns, 0, rowCount);

    chunks.emplace_back(outBuf.data(), outBuf.size());
}

class WasmDecodeSession {
public:
    WasmDecodeSession()
        : _inBuf(LARGE_BUFFER_SIZE),
          _decoder(&_inBuf, &_sink, _schemas)
    {
    }

    void decodeHeader(const Column* column) {
        const std::array<std::string_view, 1> names {"value"};
        const std::array<const Column*, 1> columns {column};

        net::proto::TuringProtoOutBuf headerBuf(LARGE_BUFFER_SIZE);
        net::proto::TuringProtoEncoder headerEncoder(&headerBuf);
        headerEncoder.writeColumnHeaders(names, columns);

        load(std::string_view(headerBuf.data(), headerBuf.size()));
        _decoder.decodeIncomingChunkHeader(&_container);
    }

    void decodeDataframe(const Column* column, size_t rowCount, size_t bufferSize) {
        std::vector<std::string> chunks;
        encodeChunks(column, rowCount, bufferSize, chunks);

        for (const std::string& chunk : chunks) {
            if (chunk.empty()) {
                continue;
            }

            load(chunk);
            _decoder.decodeIncomingChunk(&_container);
        }

        const net::proto::WireSize footer = static_cast<net::proto::WireSize>(rowCount);
        load(std::string_view(reinterpret_cast<const char*>(&footer), sizeof(footer)));
        _decoder.decodeChunkFooter(&_container);
    }

    void endChunk() {
        _container.clearColumnData();

        for (net::proto::DecodedColumnSchema& schema : _schemas) {
            schema.getColumnState().reset();
        }

        _decoder.reset();
    }

    wasm::ColumnContainer& getContainer() { return _container; }

private:
    net::proto::TuringProtoInBuf _inBuf;
    wasm::WasmSink _sink;
    wasm::ColumnContainer _container;
    std::vector<net::proto::DecodedColumnSchema> _schemas;
    net::proto::TuringProtoDecoder<wasm::WasmSink> _decoder;

    void load(std::string_view bytes) {
        _inBuf.reset();
        std::copy_n(bytes.data(), bytes.size(), _inBuf.data());
        _inBuf.increaseWriteOffset(bytes.size());
    }
};

template <typename T>
void expectDecodedRows(wasm::ColumnContainer& container, const std::vector<std::optional<T>>& rows) {
    ASSERT_EQ(container.size(), 1u);
    ASSERT_EQ(container[0]->getKind(), wasm::ColumnKind::OPTIONAL_VECTOR);

    const wasm::WasmSink::ColumnOptVector<T>* decoded = static_cast<const wasm::WasmSink::ColumnOptVector<T>*>(container[0]);
    const std::vector<T>& values = decoded->getValues();
    const DynamicLargeBitMask<uint64_t>& validity = decoded->getValidity();

    ASSERT_EQ(decoded->size(), rows.size());
    ASSERT_EQ(values.size(), rows.size());
    ASSERT_EQ(validity.size(), rows.size());

    for (size_t row = 0; row < rows.size(); row++) {
        SCOPED_TRACE(::testing::Message() << "row=" << row);
        EXPECT_EQ(validity.test(row), rows[row].has_value());
        EXPECT_EQ(decoded->isValid(row), rows[row].has_value());
        EXPECT_EQ(values[row], rows[row].value_or(T {}));
    }

    const size_t tailBits = rows.size() % 64;
    if (tailBits != 0) {
        const uint64_t lastWord = validity.data()[validity.getWordCount() - 1];
        EXPECT_EQ(lastWord >> tailBits, uint64_t {0});
    }
}

template <typename T>
void expectDecodesIntoValuesAndValidity() {
    for (const size_t rowCount : ROW_COUNTS) {
        for (const size_t bufferSize : BUFFER_SIZES) {
            SCOPED_TRACE(::testing::Message() << "rowCount=" << rowCount << " bufferSize=" << bufferSize);

            std::vector<std::optional<T>> firstRows;
            makeRows<T>(rowCount, 3, firstRows);
            ColumnOptVector<T> first;
            fillColumn<T>(&first, firstRows);

            std::vector<std::optional<T>> secondRows;
            makeRows<T>(rowCount + 5, 0, secondRows);
            ColumnOptVector<T> second;
            fillColumn<T>(&second, secondRows);

            WasmDecodeSession session;
            session.decodeHeader(&first);

            session.decodeDataframe(&first, firstRows.size(), bufferSize);
            expectDecodedRows<T>(session.getContainer(), firstRows);

            session.endChunk();
            session.decodeDataframe(&second, secondRows.size(), bufferSize);
            expectDecodedRows<T>(session.getContainer(), secondRows);
        }
    }
}

}

TEST(WasmOptionalColumnDecodeTest, Int64) {
    expectDecodesIntoValuesAndValidity<Int64>();
}

TEST(WasmOptionalColumnDecodeTest, Double) {
    expectDecodesIntoValuesAndValidity<Double>();
}

TEST(WasmOptionalColumnDecodeTest, Bool) {
    expectDecodesIntoValuesAndValidity<Bool>();
}

TEST(WasmOptionalColumnDecodeTest, String) {
    expectDecodesIntoValuesAndValidity<String>();
}
