#include <gtest/gtest.h>

#include <stdint.h>
#include <array>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "TuringProtoDecoder.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "TuringSink.h"
#include "TuringSinkColumnContainer.h"
#include "TuringValueStore.h"
#include "LocalMemory.h"
#include "columns/ColumnOptVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

using Int64 = types::Int64::Primitive;
using Double = types::Double::Primitive;
using Bool = types::Bool::Primitive;
using DateTime = types::DateTime::Primitive;

constexpr size_t LARGE_BUFFER_SIZE = 1 << 20;

constexpr std::array<size_t, 11> ROW_COUNTS {1, 7, 8, 9, 63, 64, 65, 100, 128, 130, 200};

enum class NullPattern {
    None,
    Some,
    All,
};

constexpr std::array<NullPattern, 3> NULL_PATTERNS {NullPattern::None, NullPattern::Some, NullPattern::All};

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
DateTime makeValue<DateTime>(size_t row) {
    return DateTime {static_cast<int64_t>(row) * 3600000000LL - 86400000000LL};
}

bool isNullRow(NullPattern pattern, size_t row) {
    switch (pattern) {
        case NullPattern::None:
            return false;
        break;
        case NullPattern::Some:
            return row % 3 == 1 || row % 64 == 63;
        break;
        case NullPattern::All:
            return true;
        break;
    }

    return false;
}

template <typename T>
void makeRows(NullPattern pattern, size_t rowCount, std::vector<std::optional<T>>& rows) {
    for (size_t row = 0; row < rowCount; row++) {
        if (isNullRow(pattern, row)) {
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

void appendBytes(std::string& out, const void* data, size_t size) {
    out.append(static_cast<const char*>(data), size);
}

// The wire layout of a nullable fixed-width column, written out by hand: the row count,
// the validity mask as little-endian 64-bit words with row i at bit i % 64 of word i / 64
// and the bits past the last row clear, then one value per row, zero for a null one.
template <typename T>
void expectedWireBytes(std::span<const std::optional<T>> rows, std::string& out) {
    const net::proto::WireSize rowCount = static_cast<net::proto::WireSize>(rows.size());
    appendBytes(out, &rowCount, sizeof(rowCount));

    const size_t wordCount = (rows.size() + 63) / 64;
    for (size_t wordIndex = 0; wordIndex < wordCount; wordIndex++) {
        uint64_t word = 0;
        for (size_t bit = 0; bit < 64; bit++) {
            const size_t row = wordIndex * 64 + bit;
            if (row < rows.size() && rows[row].has_value()) {
                word |= uint64_t {1} << bit;
            }
        }

        std::array<char, sizeof(word)> wordBytes {};
        for (size_t byte = 0; byte < sizeof(word); byte++) {
            wordBytes[byte] = static_cast<char>((word >> (8 * byte)) & 0xFF);
        }
        out.append(wordBytes.data(), wordBytes.size());
    }

    for (const std::optional<T>& row : rows) {
        const T value = row.value_or(T {});
        appendBytes(out, &value, sizeof(T));
    }
}

// The encoder's per-row path, kept to compare where each buffer flush falls.
template <typename T>
void writeRowByRow(net::proto::TuringProtoOutBuf* outBuf, std::span<const std::optional<T>> rows) {
    const net::proto::WireSize rowCount = static_cast<net::proto::WireSize>(rows.size());
    outBuf->copyFixedLenData(&rowCount, sizeof(rowCount));

    DynamicLargeBitMask<uint64_t> mask(rows.size());
    for (size_t row = 0; row < rows.size(); row++) {
        mask.set(row, rows[row].has_value());
    }
    outBuf->copyVarLenData(mask.data(), mask.byteSize());

    for (const std::optional<T>& row : rows) {
        const T value = row.value_or(T {});
        outBuf->copyFixedLenData(&value, sizeof(T));
    }
}

void collectFlushes(net::proto::TuringProtoOutBuf* outBuf, std::vector<std::string>* flushes) {
    outBuf->setOnBufferFullCallBack([outBuf, flushes]() {
        flushes->emplace_back(outBuf->data(), outBuf->size());
        outBuf->reset();
    });
}

void encodeColumn(const Column* column, size_t offset, size_t rowCount, size_t bufferSize, std::vector<std::string>& flushes) {
    net::proto::TuringProtoOutBuf outBuf(bufferSize);
    collectFlushes(&outBuf, &flushes);

    net::proto::TuringProtoEncoder encoder(&outBuf);
    const std::array<const Column*, 1> columns {column};
    encoder.writeColumns(columns, offset, rowCount);

    flushes.emplace_back(outBuf.data(), outBuf.size());
}

template <typename T>
void encodeRowByRow(std::span<const std::optional<T>> rows, size_t bufferSize, std::vector<std::string>& flushes) {
    net::proto::TuringProtoOutBuf outBuf(bufferSize);
    collectFlushes(&outBuf, &flushes);

    writeRowByRow<T>(&outBuf, rows);

    flushes.emplace_back(outBuf.data(), outBuf.size());
}

std::string concatenate(const std::vector<std::string>& flushes) {
    std::string out;
    for (const std::string& flush : flushes) {
        out += flush;
    }
    return out;
}

template <typename T>
void expectEncodesLikeTheWireLayout(NullPattern pattern, size_t rowCount) {
    std::vector<std::optional<T>> rows;
    makeRows<T>(pattern, rowCount, rows);

    ColumnOptVector<T> column;
    fillColumn<T>(&column, rows);

    for (const size_t offset : std::array<size_t, 4> {0, 1, 63, 64}) {
        if (offset >= rowCount) {
            continue;
        }

        const size_t windowRowCount = rowCount - offset;
        const std::span<const std::optional<T>> window = std::span<const std::optional<T>>(rows).subspan(offset);
        SCOPED_TRACE(::testing::Message() << "offset=" << offset << " rows=" << windowRowCount);

        std::string expected;
        expectedWireBytes<T>(window, expected);

        std::vector<std::string> encoded;
        encodeColumn(&column, offset, windowRowCount, LARGE_BUFFER_SIZE, encoded);
        EXPECT_EQ(concatenate(encoded), expected);

        for (const size_t bufferSize : std::array<size_t, 4> {13, 24, 37, 64}) {
            SCOPED_TRACE(::testing::Message() << "bufferSize=" << bufferSize);

            std::vector<std::string> rowByRow;
            encodeRowByRow<T>(window, bufferSize, rowByRow);

            std::vector<std::string> flushes;
            encodeColumn(&column, offset, windowRowCount, bufferSize, flushes);
            EXPECT_EQ(flushes, rowByRow);
        }
    }
}

void roundTrip(const Column* column, size_t rowCount, size_t bufferSize, LocalMemory* localMemory, net::proto::TuringValueStore* values, DataframeManager* dataframeManager, Dataframe* decoded) {
    std::vector<std::string> chunks;
    encodeColumn(column, 0, rowCount, bufferSize, chunks);

    const std::array<std::string_view, 1> names {"value"};
    const std::array<const Column*, 1> columns {column};
    net::proto::TuringProtoOutBuf headerBuf(LARGE_BUFFER_SIZE);
    net::proto::TuringProtoEncoder headerEncoder(&headerBuf);
    headerEncoder.writeColumnHeaders(names, columns);

    std::vector<net::proto::DecodedColumnSchema> schemas;

    net::proto::TuringProtoInBuf inBuf(LARGE_BUFFER_SIZE);
    net::proto::TuringSink sink(localMemory, values);
    net::proto::TuringSinkColumnContainer container(decoded, dataframeManager);
    net::proto::TuringProtoDecoder<net::proto::TuringSink> decoder(&inBuf, &sink, schemas);

    const auto load = [&inBuf](std::string_view bytes) {
        inBuf.reset();
        std::copy_n(bytes.data(), bytes.size(), inBuf.data());
        inBuf.increaseWriteOffset(bytes.size());
    };

    load(std::string_view(headerBuf.data(), headerBuf.size()));
    decoder.decodeIncomingChunkHeader(&container);

    for (const std::string& chunk : chunks) {
        if (chunk.empty()) {
            continue;
        }

        load(chunk);
        decoder.decodeIncomingChunk(&container);
    }

    const net::proto::WireSize footer = static_cast<net::proto::WireSize>(rowCount);
    load(std::string_view(reinterpret_cast<const char*>(&footer), sizeof(footer)));
    decoder.decodeChunkFooter(&container);
}

template <typename T>
void expectRoundTrips(NullPattern pattern, size_t rowCount) {
    std::vector<std::optional<T>> rows;
    makeRows<T>(pattern, rowCount, rows);

    for (const size_t bufferSize : std::array<size_t, 3> {24, 97, LARGE_BUFFER_SIZE}) {
        SCOPED_TRACE(::testing::Message() << "bufferSize=" << bufferSize);

        LocalMemory localMemory;
        net::proto::TuringValueStore values;
        ColumnOptVector<T> column;
        fillColumn<T>(&column, rows);

        DataframeManager dataframeManager;
        Dataframe decoded;
        roundTrip(&column, rowCount, bufferSize, &localMemory, &values, &dataframeManager, &decoded);

        ASSERT_EQ(decoded.cols().size(), 1u);
        const ColumnOptVector<T>* decodedColumn = decoded.cols().at(0)->as<ColumnOptVector<T>>();
        ASSERT_NE(decodedColumn, nullptr);
        ASSERT_EQ(decodedColumn->size(), rowCount);

        size_t nullCount = 0;
        for (size_t row = 0; row < rowCount; row++) {
            SCOPED_TRACE(::testing::Message() << "row=" << row);
            EXPECT_EQ(decodedColumn->get(row), rows[row]);

            if (!rows[row].has_value()) {
                EXPECT_EQ(decodedColumn->getValues()[row], T {});
                nullCount++;
            }
        }
        EXPECT_EQ(decodedColumn->getNullCount(), nullCount);
    }
}

template <typename T>
void checkEveryShape() {
    for (const NullPattern pattern : NULL_PATTERNS) {
        for (const size_t rowCount : ROW_COUNTS) {
            SCOPED_TRACE(::testing::Message() << "pattern=" << static_cast<int>(pattern) << " rowCount=" << rowCount);
            expectEncodesLikeTheWireLayout<T>(pattern, rowCount);
            expectRoundTrips<T>(pattern, rowCount);
        }
    }
}

}

TEST(TuringProtoNullableBufferTest, Int64) {
    checkEveryShape<Int64>();
}

TEST(TuringProtoNullableBufferTest, Double) {
    checkEveryShape<Double>();
}

TEST(TuringProtoNullableBufferTest, Bool) {
    checkEveryShape<Bool>();
}

TEST(TuringProtoNullableBufferTest, DateTime) {
    checkEveryShape<DateTime>();
}

TEST(TuringProtoNullableBufferTest, KeepsTheWireLayoutAfterAValueIsNulled) {
    ColumnOptVector<Int64> column;
    for (size_t row = 0; row < 70; row++) {
        column.push_back(static_cast<Int64>(row) + 100);
    }
    column.setNull(3);
    column.setNull(66);
    column.set(3, Int64 {-5});

    std::vector<std::optional<Int64>> rows;
    for (size_t row = 0; row < 70; row++) {
        rows.push_back(column.get(row));
    }

    std::string expected;
    expectedWireBytes<Int64>(rows, expected);

    std::vector<std::string> encoded;
    encodeColumn(&column, 0, 70, LARGE_BUFFER_SIZE, encoded);
    EXPECT_EQ(concatenate(encoded), expected);
}

TEST(TuringProtoNullableBufferTest, SetValidityClearsNullValuesAndTheBitsPastTheLastRow) {
    ColumnOptVector<Int64> column;
    for (size_t row = 0; row < 70; row++) {
        column.push_back(static_cast<Int64>(row) + 1);
    }

    DynamicLargeBitMask<uint64_t> validity(70);
    validity.data()[0] = ~uint64_t {0} ^ (uint64_t {1} << 5);
    validity.data()[1] = ~uint64_t {0} ^ (uint64_t {1} << 3);
    column.setValidity(validity);

    EXPECT_EQ(column.getNullCount(), 2u);
    EXPECT_FALSE(column.isValid(5));
    EXPECT_FALSE(column.isValid(67));
    EXPECT_EQ(column.getValues()[5], Int64 {0});
    EXPECT_EQ(column.getValues()[67], Int64 {0});
    EXPECT_EQ(column.get(69), std::optional<Int64> {70});

    column.resize(72);
    EXPECT_EQ(column.getNullCount(), 4u);
    EXPECT_FALSE(column.isValid(70));
    EXPECT_FALSE(column.isValid(71));

    DynamicLargeBitMask<uint64_t> allValid(72);
    allValid.fill(0, 72, true);
    column.setValidity(allValid);
    EXPECT_EQ(column.getNullCount(), 0u);
    EXPECT_FALSE(column.hasMask());
}
