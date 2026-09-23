#include <gtest/gtest.h>
#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ChunkedBuffer.h"
#include "TuringException.h"
#include "TuringProtoDecoder.h"
#include "TuringSink.h"
#include "TuringSinkColumnContainer.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "LocalMemory.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"

namespace {

using UInt64 = db::types::UInt64::Primitive;
using Int64 = db::types::Int64::Primitive;
using StringView = db::types::String::Primitive;
using Bool = db::types::Bool::Primitive;
using Embedding = db::types::Embedding::Primitive;
using DateTime = db::types::DateTime::Primitive;

struct FramedPacket {
    net::proto::MessageTypes _type;
    std::string _bytes;
};

template <typename ColumnT>
void addColumn(db::DataframeManager* dfMan, db::Dataframe* df, std::string_view name, ColumnT* column) {
    auto* namedColumn = db::NamedColumn::create(dfMan, column, dfMan->allocTag());
    namedColumn->rename(name);
    df->addColumn(namedColumn);
}

std::string framePacket(net::proto::MessageTypes type, std::string_view payload) {
    net::proto::TuringProtoOutBuf packetBuf(
        net::proto::ProtoHeader::wireSize() + payload.size());
    packetBuf.setOnBufferFullCallBack([]() {});
    net::proto::frameMessage(type, payload, &packetBuf);
    return std::string(packetBuf.data(), packetBuf.size());
}

size_t countPacketsOfType(const std::vector<FramedPacket>& packets,
                          net::proto::MessageTypes type);

// Drive the encoder with tiny buffers so the tests can force the same
// CHUNK_HEADER/CHUNK/END_CHUNK boundaries that the network path emits.
std::vector<FramedPacket> encodeDataframeWithChunkSize(const db::Dataframe& df, size_t chunkSize) {
    std::vector<std::string_view> names;
    std::vector<const db::Column*> columns;
    for (const db::NamedColumn* namedColumn : df.cols()) {
        names.push_back(namedColumn->getName());
        columns.push_back(namedColumn->getColumn());
    }

    std::vector<FramedPacket> packets;
    net::proto::TuringProtoOutBuf schemaBuf(chunkSize);
    net::proto::TuringProtoOutBuf dataBuf(chunkSize);

    auto emitPacket = [&](net::proto::MessageTypes type, net::proto::TuringProtoOutBuf* buf) {
        packets.push_back(FramedPacket {
            ._type = type,
            ._bytes = framePacket(type, std::string_view(buf->data(), buf->size()))});
        buf->reset();
    };

    {
        net::proto::TuringProtoEncoder encoder(&schemaBuf);
        encoder.writeColumnHeaders(names, columns);
        emitPacket(net::proto::MessageTypes::CHUNK_HEADER, &schemaBuf);
    }

    net::proto::TuringProtoEncoder encoder(&dataBuf);
    dataBuf.setOnBufferFullCallBack([&]() {
        emitPacket(net::proto::MessageTypes::CHUNK, &dataBuf);
    });

    const size_t rowCount = df.getLogicalRowCount();
    encoder.writeColumns(columns, 0, rowCount);
    if (dataBuf.size() > 0) {
        emitPacket(net::proto::MessageTypes::CHUNK, &dataBuf);
    }
    encoder.writeChunkFooter(rowCount);
    packets.push_back(FramedPacket {
        ._type = net::proto::MessageTypes::END_CHUNK,
        ._bytes = framePacket(net::proto::MessageTypes::END_CHUNK,
                              std::string_view(dataBuf.data(), dataBuf.size()))});

    return packets;
}

// Every dataframe should emit exactly one schema packet up front and one
// terminator at the end, regardless of how many data chunks are needed.
void expectPacketSequence(const std::vector<FramedPacket>& packets, bool expectDataPackets) {
    ASSERT_GE(packets.size(), 2u);
    EXPECT_EQ(packets.front()._type, net::proto::MessageTypes::CHUNK_HEADER);
    EXPECT_EQ(packets.back()._type, net::proto::MessageTypes::END_CHUNK);
    EXPECT_EQ(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK_HEADER), 1u);

    if (expectDataPackets) {
        EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 1u);
    } else {
        EXPECT_EQ(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 0u);
    }
}

size_t countPacketsOfType(const std::vector<FramedPacket>& packets,
                          net::proto::MessageTypes type) {
    return std::count_if(packets.begin(), packets.end(), [&](const FramedPacket& packet) {
        return packet._type == type;
    });
}

// Replay packets exactly as the receiver would consume them; the decoder keeps
// schema state across chunks, so preserving packet order matters here.
void decodeChunkPackets(const std::vector<FramedPacket>& packets,
                        db::LocalMemory* localMem,
                        net::proto::ChunkedBuffer<float>* embeddingBuffer,
                        net::proto::ChunkedBuffer<char>* stringBuffer,
                        db::ListBuffer<>* listBuffer,
                        db::MapBuffer<>* mapBuffer,
                        db::DataframeManager* dfMan,
                        db::Dataframe* decoded,
                        std::vector<net::proto::DecodedColumnSchema>* schemas) {
    const size_t maxPayloadSize =
        std::transform_reduce(packets.begin(), packets.end(), size_t {0}, [](size_t lhs, size_t rhs) { return std::max(lhs, rhs); }, [](const FramedPacket& packet) { return packet._bytes.size() - net::proto::ProtoHeader::wireSize(); });
    net::proto::TuringProtoInBuf inBuf(maxPayloadSize);
    net::proto::TuringSink sink(localMem, embeddingBuffer, stringBuffer, listBuffer, mapBuffer);
    net::proto::TuringSinkColumnContainer decodedContainer(decoded, dfMan);
    net::proto::TuringProtoDecoder<net::proto::TuringSink> decoder(&inBuf, &sink, *schemas);
    schemas->clear();

    for (const auto& packet : packets) {
        const auto protoHeader = net::proto::ProtoHeader::decode(packet._bytes.data(), packet._bytes.size());
        EXPECT_EQ(protoHeader._type, packet._type);

        inBuf.reset();
        std::memcpy(inBuf.data(),
                    packet._bytes.data() + net::proto::ProtoHeader::wireSize(),
                    protoHeader._dataLen);
        inBuf.increaseWriteOffset(protoHeader._dataLen);

        switch (packet._type) {
            case net::proto::MessageTypes::CHUNK_HEADER:
                decoder.decodeIncomingChunkHeader(&decodedContainer);
                break;
            case net::proto::MessageTypes::CHUNK:
                decoder.decodeIncomingChunk(&decodedContainer);
                break;
            case net::proto::MessageTypes::END_CHUNK:
                decoder.decodeChunkFooter(&decodedContainer);
                break;
            default:
                FAIL() << "Unexpected packet type in round-trip decode";
                break;
        }
    }
}

std::vector<float> makeHugeEmbedding(size_t dimension) {
    std::vector<float> embedding(dimension);
    std::iota(embedding.begin(), embedding.end(), 0.5f);
    return embedding;
}

void expectEmbedding(Embedding actual, std::span<const float> expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        EXPECT_FLOAT_EQ(actual[i], expected[i]);
    }
}

} // namespace

// Encode a dataframe of two fixed-width numeric columns (UInt64 ids,
// signed Int64 scores) and decode it back. The chunk size is varied so the
// encoder is forced to flush the data column across one CHUNK packet (large
// chunks) up to several (small chunks). The decoded column values must
// match bit-for-bit, proving the chunk-boundary stitching is correct for
// the simplest column kind.
TEST(TuringProtoRoundTripTest, RoundTripsNumericColumnsAcrossChunkSizes) {
    for (const size_t chunkSize : std::array<size_t, 4> {48, 64, 97, 256}) {
        SCOPED_TRACE(::testing::Message() << "chunkSize=" << chunkSize);

        db::LocalMemory localMem;
        db::DataframeManager dfMan;
        db::Dataframe source;

        auto* ids = localMem.alloc<db::ColumnVector<UInt64>>();
        ids->push_back(1);
        ids->push_back(2);
        ids->push_back(3);
        ids->push_back(4);
        ids->push_back(5);
        ids->push_back(6);
        addColumn(&dfMan, &source, "id", ids);

        auto* scores = localMem.alloc<db::ColumnVector<Int64>>();
        scores->push_back(-10);
        scores->push_back(25);
        scores->push_back(99);
        scores->push_back(-42);
        scores->push_back(0);
        scores->push_back(7);
        addColumn(&dfMan, &source, "score", scores);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::MapBuffer<> mapBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 2u);
        EXPECT_EQ(decoded.getLogicalRowCount(), 6u);
        EXPECT_EQ(decoded.cols().at(0)->getName(), "id");
        EXPECT_EQ(decoded.cols().at(1)->getName(), "score");

        const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnVector<UInt64>>();
        const auto* decodedScores = decoded.cols().at(1)->as<db::ColumnVector<Int64>>();
        ASSERT_NE(decodedIds, nullptr);
        ASSERT_NE(decodedScores, nullptr);

        EXPECT_EQ(decodedIds->getRaw(),
                  (std::vector<UInt64> {1, 2, 3, 4, 5, 6}));
        EXPECT_EQ(decodedScores->getRaw(),
                  (std::vector<Int64> {-10, 25, 99, -42, 0, 7}));
    }
}

// A datetime is eight bytes on the wire like an integer, but under its own type code, so
// what this pins is that the decoder rebuilds a DateTime column rather than the Int64 it
// would be indistinguishable from otherwise. The nullable shape is the one a property read
// produces, so both go over.
TEST(TuringProtoRoundTripTest, RoundTripsDateTimeColumnsAcrossChunkSizes) {
    using OptionalDateTime = std::optional<DateTime>;

    constexpr int64_t microsecondsPerHour = 3600LL * 1000000;

    for (const size_t chunkSize : std::array<size_t, 4> {48, 64, 97, 256}) {
        SCOPED_TRACE(::testing::Message() << "chunkSize=" << chunkSize);

        db::LocalMemory localMem;
        db::DataframeManager dfMan;
        db::Dataframe source;

        // One instant before the epoch, so the negative count crosses the wire too
        auto* created = localMem.alloc<db::ColumnVector<DateTime>>();
        created->push_back(DateTime {0});
        created->push_back(DateTime {microsecondsPerHour});
        created->push_back(DateTime {-microsecondsPerHour});
        created->push_back(DateTime {1790172300LL * 1000000});
        addColumn(&dfMan, &source, "created", created);

        auto* joined = localMem.alloc<db::ColumnOptVector<DateTime>>();
        joined->push_back(DateTime {microsecondsPerHour});
        joined->push_back(std::nullopt);
        joined->push_back(DateTime {2 * microsecondsPerHour});
        joined->push_back(std::nullopt);
        addColumn(&dfMan, &source, "joined", joined);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::MapBuffer<> mapBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 2u);
        EXPECT_EQ(decoded.getLogicalRowCount(), 4u);

        const auto* decodedCreated = decoded.cols().at(0)->as<db::ColumnVector<DateTime>>();
        const auto* decodedJoined = decoded.cols().at(1)->as<db::ColumnOptVector<DateTime>>();
        ASSERT_NE(decodedCreated, nullptr);
        ASSERT_NE(decodedJoined, nullptr);

        EXPECT_EQ(decodedCreated->getRaw(),
                  (std::vector<DateTime> {DateTime {0},
                                          DateTime {microsecondsPerHour},
                                          DateTime {-microsecondsPerHour},
                                          DateTime {1790172300LL * 1000000}}));

        EXPECT_EQ(decodedJoined->getRaw(),
                  (std::vector<OptionalDateTime> {DateTime {microsecondsPerHour},
                                                  std::nullopt,
                                                  DateTime {2 * microsecondsPerHour},
                                                  std::nullopt}));
    }
}

// A label predicate answers with a mask, a boolean column that carries no nulls of its
// own, so it goes on the wire as a plain BOOL vector.
TEST(TuringProtoRoundTripTest, RoundTripsMaskColumns) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    auto* isPerson = localMem.alloc<db::ColumnMask>();
    isPerson->push_back(true);
    isPerson->push_back(false);
    isPerson->push_back(false);
    isPerson->push_back(true);
    addColumn(&dfMan, &source, "n:Person", isPerson);

    const auto packets = encodeDataframeWithChunkSize(source, 64);
    expectPacketSequence(packets, true);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    EXPECT_EQ(decoded.getLogicalRowCount(), 4u);
    EXPECT_EQ(decoded.cols().at(0)->getName(), "n:Person");

    const auto* decodedFlags = decoded.cols().at(0)->as<db::ColumnVector<Bool>>();
    ASSERT_NE(decodedFlags, nullptr);

    EXPECT_EQ(decodedFlags->getRaw(), (std::vector<Bool> {true, false, false, true}));
}

// Optional string columns combine two harder cases: variable-length data
// (so length prefixes flow through the wire) and a null mask (so the
// presence bits must align with the values on decode). One of the strings
// is deliberately long enough that, at the smaller chunk sizes, it spans a
// CHUNK boundary mid-string. Verifies the decoder reassembles it correctly
// and preserves nulls in the right positions.
TEST(TuringProtoRoundTripTest, RoundTripsOptionalStringColumnsAcrossChunkSizes) {
    using OptionalString = std::optional<StringView>;
    using namespace std::string_view_literals;

    for (const size_t chunkSize : std::array<size_t, 4> {48, 63, 95, 192}) {
        SCOPED_TRACE(::testing::Message() << "chunkSize=" << chunkSize);

        db::LocalMemory localMem;
        db::DataframeManager dfMan;
        db::Dataframe source;

        auto* ids = localMem.alloc<db::ColumnVector<UInt64>>();
        ids->push_back(101);
        ids->push_back(102);
        ids->push_back(103);
        ids->push_back(104);
        addColumn(&dfMan, &source, "id", ids);

        auto* labels = localMem.alloc<db::ColumnOptVector<StringView>>();
        labels->push_back(OptionalString {"alpha"sv});
        labels->push_back(std::nullopt);
        labels->push_back(OptionalString {"this string is deliberately long enough to cross chunk boundaries"sv});
        labels->push_back(OptionalString {"omega"sv});
        addColumn(&dfMan, &source, "label", labels);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::MapBuffer<> mapBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 2u);
        EXPECT_EQ(decoded.getLogicalRowCount(), 4u);

        const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnVector<UInt64>>();
        const auto* decodedLabels = decoded.cols().at(1)->as<db::ColumnOptVector<StringView>>();
        ASSERT_NE(decodedIds, nullptr);
        ASSERT_NE(decodedLabels, nullptr);

        EXPECT_EQ(decodedIds->getRaw(),
                  (std::vector<UInt64> {101, 102, 103, 104}));
        EXPECT_EQ(decodedLabels->getRaw(),
                  (std::vector<OptionalString> {
                      "alpha"sv,
                      std::nullopt,
                      "this string is deliberately long enough to cross chunk boundaries"sv,
                      "omega"sv}));
    }
}

// Stress the chunked-string path with a single value that is several times
// the chunk size, so it cannot live in one CHUNK packet and the encoder
// must split it. Asserts that at least 5 CHUNK packets were emitted (proves
// the split actually happened) and that the full string round-trips to the
// decoder side with no loss.
// A property column of lists: every row carries a list header, so a null one is read and
// reserved like any other and simply leaves its entry empty. Small chunk sizes force the
// header and the elements of one row to land in different packets.
TEST(TuringProtoRoundTripTest, RoundTripsOptionalListColumnsAcrossChunkSizes) {
    using OptionalList = std::optional<db::ListView>;
    using namespace std::string_view_literals;

    for (const size_t chunkSize : std::array<size_t, 4> {48, 63, 95, 192}) {
        SCOPED_TRACE(::testing::Message() << "chunkSize=" << chunkSize);

        db::LocalMemory localMem;
        db::DataframeManager dfMan;
        db::Dataframe source;

        std::vector<db::ListBuffer<>::ListItemVariant> firstItems;
        firstItems.emplace_back(Int64 {1});
        firstItems.emplace_back(StringView {"this element is deliberately long enough to cross chunk boundaries"sv});
        const db::ListView first = localMem.listBuffer().insert(firstItems);

        const std::vector<db::ListBuffer<>::ListItemVariant> emptyItems;
        const db::ListView empty = localMem.listBuffer().insert(emptyItems);

        std::vector<db::ListBuffer<>::ListItemVariant> lastItems;
        lastItems.emplace_back(Int64 {7});
        lastItems.emplace_back(Int64 {8});
        const db::ListView last = localMem.listBuffer().insert(lastItems);

        auto* tags = localMem.alloc<db::ColumnOptVector<db::ListView>>();
        tags->push_back(OptionalList {first});
        tags->push_back(std::nullopt);
        tags->push_back(OptionalList {empty});
        tags->push_back(OptionalList {last});
        addColumn(&dfMan, &source, "tags", tags);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::MapBuffer<> mapBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 1u);
        EXPECT_EQ(decoded.getLogicalRowCount(), 4u);

        const auto* decodedTags = decoded.cols().at(0)->as<db::ColumnOptVector<db::ListView>>();
        ASSERT_NE(decodedTags, nullptr);

        const std::vector<OptionalList>& rows = decodedTags->getRaw();
        ASSERT_EQ(rows.size(), 4u);

        ASSERT_TRUE(rows[0].has_value());
        ASSERT_EQ(rows[0]->size(), 2u);
        EXPECT_EQ(rows[0]->front().getAs<Int64>(), 1);
        EXPECT_EQ(rows[0]->back().getAs<StringView>(),
                  "this element is deliberately long enough to cross chunk boundaries"sv);

        EXPECT_FALSE(rows[1].has_value());

        ASSERT_TRUE(rows[2].has_value());
        EXPECT_EQ(rows[2]->size(), 0u);

        ASSERT_TRUE(rows[3].has_value());
        ASSERT_EQ(rows[3]->size(), 2u);
        EXPECT_EQ(rows[3]->front().getAs<Int64>(), 7);
        EXPECT_EQ(rows[3]->back().getAs<Int64>(), 8);
    }
}

TEST(TuringProtoRoundTripTest, RoundTripsHugeStringsAcrossMultipleBuffers) {
    for (const size_t chunkSize : std::array<size_t, 2> {64, 96}) {
        SCOPED_TRACE(::testing::Message() << "chunkSize=" << chunkSize);

        db::LocalMemory localMem;
        db::DataframeManager dfMan;
        db::Dataframe source;

        auto* ids = localMem.alloc<db::ColumnVector<UInt64>>();
        ids->push_back(1);
        addColumn(&dfMan, &source, "id", ids);

        const std::string hugeLabel(chunkSize * 5 + 37, 'x');
        auto* labels = localMem.alloc<db::ColumnVector<StringView>>();
        labels->push_back(hugeLabel);
        addColumn(&dfMan, &source, "label", labels);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);
        EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 5u);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::MapBuffer<> mapBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 2u);
        const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnVector<UInt64>>();
        const auto* decodedLabels = decoded.cols().at(1)->as<db::ColumnVector<StringView>>();
        ASSERT_NE(decodedIds, nullptr);
        ASSERT_NE(decodedLabels, nullptr);

        EXPECT_EQ(decodedIds->getRaw(), (std::vector<UInt64> {1}));
        ASSERT_EQ(decodedLabels->size(), 1u);
        EXPECT_EQ(decodedLabels->at(0), hugeLabel);
    }
}

// Embeddings (variable-length float arrays) take a separate code path from
// strings on both encode and decode, so this is the analog of the huge
// string test for vector data. Builds an embedding large enough to cross
// many CHUNK boundaries and verifies that every element survives the
// round-trip with float-equality tolerance.
TEST(TuringProtoRoundTripTest, RoundTripsHugeEmbeddingsAcrossMultipleBuffers) {
    for (const size_t chunkSize : std::array<size_t, 2> {64, 128}) {
        SCOPED_TRACE(::testing::Message() << "chunkSize=" << chunkSize);

        db::LocalMemory localMem;
        db::DataframeManager dfMan;
        db::Dataframe source;

        auto* ids = localMem.alloc<db::ColumnVector<UInt64>>();
        ids->push_back(99);
        addColumn(&dfMan, &source, "id", ids);

        const auto hugeEmbedding = makeHugeEmbedding(chunkSize * 6 / sizeof(float) + 17);
        auto* embeddings = localMem.alloc<db::ColumnVector<Embedding>>();
        embeddings->push_back(std::span<const float>(hugeEmbedding));
        addColumn(&dfMan, &source, "vec", embeddings);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);
        EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 5u);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::MapBuffer<> mapBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 2u);
        const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnVector<UInt64>>();
        const auto* decodedEmbeddings = decoded.cols().at(1)->as<db::ColumnVector<Embedding>>();
        ASSERT_NE(decodedIds, nullptr);
        ASSERT_NE(decodedEmbeddings, nullptr);

        EXPECT_EQ(decodedIds->getRaw(), (std::vector<UInt64> {99}));
        ASSERT_EQ(decodedEmbeddings->size(), 1u);
        expectEmbedding(decodedEmbeddings->at(0), std::span<const float>(hugeEmbedding));
    }
}

// ColumnConst broadcasts a single value across all logical rows. With an
// optional payload there are three meaningful states to cover in one frame:
// a numeric some-value, a string some-value large enough to force the
// encoder to chunk it, and a nullopt. Verifies all three reach the decoder
// as ColumnConst with the correct contained value.
TEST(TuringProtoRoundTripTest, RoundTripsOptionalConstantColumns) {
    using OptionalString = std::optional<StringView>;

    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    auto* maybeId = localMem.alloc<db::ColumnConst<std::optional<UInt64>>>();
    maybeId->set(std::optional<UInt64> {42});
    addColumn(&dfMan, &source, "maybe_id", maybeId);

    const std::string hugeLabel(512, 'q');
    auto* maybeLabel = localMem.alloc<db::ColumnConst<OptionalString>>();
    maybeLabel->set(OptionalString {std::string_view(hugeLabel)});
    addColumn(&dfMan, &source, "maybe_label", maybeLabel);

    auto* emptyLabel = localMem.alloc<db::ColumnConst<OptionalString>>();
    emptyLabel->set(std::nullopt);
    addColumn(&dfMan, &source, "empty_label", emptyLabel);

    const auto packets = encodeDataframeWithChunkSize(source, 96);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 2u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 3u);
    const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnConst<std::optional<UInt64>>>();
    const auto* decodedLabel = decoded.cols().at(1)->as<db::ColumnConst<OptionalString>>();
    const auto* decodedEmptyLabel = decoded.cols().at(2)->as<db::ColumnConst<OptionalString>>();
    ASSERT_NE(decodedIds, nullptr);
    ASSERT_NE(decodedLabel, nullptr);
    ASSERT_NE(decodedEmptyLabel, nullptr);

    EXPECT_EQ(decodedIds->at(0), std::optional<UInt64> {42});
    EXPECT_EQ(decodedLabel->at(0), OptionalString {hugeLabel});
    EXPECT_EQ(decodedEmptyLabel->at(0), std::nullopt);
}

// Encode a constant list column whose single list mixes a fixed element, a string, and an
// embedding, then decode it back. The chunk size is small enough to split the string
// payload across packets, exercising the list header grouping, the fixed raw-copy path,
// and the String/Embedding side-buffer + _bufferState resume.
TEST(TuringProtoRoundTripTest, RoundTripsConstantListColumns) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    // Longer than the chunk size below, so its payload spans packets.
    const std::string text(64, 'z');
    const std::vector<float> embedding {1.0f, 2.5f, -3.0f, 4.25f};

    std::vector<db::ListBuffer<>::ListItemVariant> items;
    items.emplace_back(Int64 {-7});
    items.emplace_back(StringView {text});
    items.emplace_back(Embedding {embedding});
    items.emplace_back(UInt64 {99});

    auto* listCol = localMem.alloc<db::ColumnConst<db::ListView>>();
    listCol->set(localMem.listBuffer().insert(items));
    addColumn(&dfMan, &source, "my_list", listCol);

    const auto packets = encodeDataframeWithChunkSize(source, 48);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 2u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedList = decoded.cols().at(0)->as<db::ColumnConst<db::ListView>>();
    ASSERT_NE(decodedList, nullptr);

    const db::ListView view = decodedList->at(0);
    ASSERT_EQ(view.size(), 4u);

    auto element = view.begin();
    EXPECT_EQ(element->getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ(element->getAs<Int64>(), -7);
    ++element;
    EXPECT_EQ(element->getTag(), db::ListBufferTypeTag::String);
    EXPECT_EQ(element->getAs<StringView>(), std::string_view(text));
    ++element;
    EXPECT_EQ(element->getTag(), db::ListBufferTypeTag::Embedding);
    expectEmbedding(element->getAs<Embedding>(), std::span<const float>(embedding));
    ++element;
    EXPECT_EQ(element->getTag(), db::ListBufferTypeTag::UInt);
    EXPECT_EQ(element->getAs<UInt64>(), 99u);
}

// Encode a ColumnVector<ListElementView> — one tagged element per row — and decode it
// back. Same wire shape as a list of N elements; the small chunk size splits the string
// element's payload across packets.
TEST(TuringProtoRoundTripTest, RoundTripsListElementViewColumns) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    const std::string text(64, 'q');
    const std::vector<float> embedding {0.5f, 1.5f, 2.5f};

    std::vector<db::ListBuffer<>::ListItemVariant> items;
    items.emplace_back(Int64 {-3});
    items.emplace_back(StringView {text});
    items.emplace_back(Embedding {embedding});
    items.emplace_back(UInt64 {123});
    items.emplace_back(db::PropertyNull {});

    const db::ListView list = localMem.listBuffer().insert(items);

    auto* col = localMem.alloc<db::ColumnVector<db::ListElementView>>();
    for (const auto& element : list) {
        col->push_back(element);
    }
    addColumn(&dfMan, &source, "elements", col);

    const auto packets = encodeDataframeWithChunkSize(source, 48);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 2u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedCol = decoded.cols().at(0)->as<db::ColumnVector<db::ListElementView>>();
    ASSERT_NE(decodedCol, nullptr);
    ASSERT_EQ(decodedCol->size(), 5u);

    EXPECT_EQ(decodedCol->at(0).getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ(decodedCol->at(0).getAs<Int64>(), -3);
    EXPECT_EQ(decodedCol->at(1).getTag(), db::ListBufferTypeTag::String);
    EXPECT_EQ(decodedCol->at(1).getAs<StringView>(), std::string_view(text));
    EXPECT_EQ(decodedCol->at(2).getTag(), db::ListBufferTypeTag::Embedding);
    expectEmbedding(decodedCol->at(2).getAs<Embedding>(), std::span<const float>(embedding));
    EXPECT_EQ(decodedCol->at(3).getTag(), db::ListBufferTypeTag::UInt);
    EXPECT_EQ(decodedCol->at(3).getAs<UInt64>(), 123u);
    EXPECT_EQ(decodedCol->at(4).getTag(), db::ListBufferTypeTag::Null);
}

// Encode a ColumnConst<ListView> whose elements include a nested list, which itself
// contains a doubly-nested list, and decode it back. Exercises the inline depth-first
// encoding and the decoder's cursor stack to three levels. The small chunk size splits
// the deep string's payload across packets, so the nested element resumes mid-decode.
TEST(TuringProtoRoundTripTest, RoundTripsNestedListColumns) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    // Longer than the chunk size below, so its payload spans packets.
    const std::string deepText(64, 'd');

    // deep = [2, "ddd...d"]
    std::vector<db::ListBuffer<>::ListItemVariant> deepItems;
    deepItems.emplace_back(UInt64 {2});
    deepItems.emplace_back(StringView {deepText});
    const db::ListView deep = localMem.listBuffer().insert(deepItems);

    // inner = [1, deep, 3]
    std::vector<db::ListBuffer<>::ListItemVariant> innerItems;
    innerItems.emplace_back(Int64 {1});
    innerItems.emplace_back(deep);
    innerItems.emplace_back(Int64 {3});
    const db::ListView inner = localMem.listBuffer().insert(innerItems);

    // outer = [7, inner, "end"]
    std::vector<db::ListBuffer<>::ListItemVariant> outerItems;
    outerItems.emplace_back(Int64 {7});
    outerItems.emplace_back(inner);
    outerItems.emplace_back(StringView {"end"});
    const db::ListView outer = localMem.listBuffer().insert(outerItems);

    auto* listCol = localMem.alloc<db::ColumnConst<db::ListView>>();
    listCol->set(outer);
    addColumn(&dfMan, &source, "nested", listCol);

    const auto packets = encodeDataframeWithChunkSize(source, 48);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 2u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedList = decoded.cols().at(0)->as<db::ColumnConst<db::ListView>>();
    ASSERT_NE(decodedList, nullptr);

    const db::ListView outerView = decodedList->at(0);
    ASSERT_EQ(outerView.size(), 3u);

    auto outerIt = outerView.begin();
    EXPECT_EQ(outerIt->getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ(outerIt->getAs<Int64>(), 7);

    ++outerIt;
    EXPECT_EQ(outerIt->getTag(), db::ListBufferTypeTag::ListView);
    const db::ListView innerView = outerIt->getAs<db::ListView>();
    ASSERT_EQ(innerView.size(), 3u);

    auto innerIt = innerView.begin();
    EXPECT_EQ(innerIt->getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ(innerIt->getAs<Int64>(), 1);

    ++innerIt;
    EXPECT_EQ(innerIt->getTag(), db::ListBufferTypeTag::ListView);
    const db::ListView deepView = innerIt->getAs<db::ListView>();
    ASSERT_EQ(deepView.size(), 2u);

    auto deepIt = deepView.begin();
    EXPECT_EQ(deepIt->getTag(), db::ListBufferTypeTag::UInt);
    EXPECT_EQ(deepIt->getAs<UInt64>(), 2u);
    ++deepIt;
    EXPECT_EQ(deepIt->getTag(), db::ListBufferTypeTag::String);
    EXPECT_EQ(deepIt->getAs<StringView>(), std::string_view(deepText));

    ++innerIt;
    EXPECT_EQ(innerIt->getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ(innerIt->getAs<Int64>(), 3);

    ++outerIt;
    EXPECT_EQ(outerIt->getTag(), db::ListBufferTypeTag::String);
    EXPECT_EQ(outerIt->getAs<StringView>(), std::string_view("end"));
}

// Encode a ColumnVector<ListElementView> whose every row is itself a list (nested), one
// of them empty, and decode it back. Each row is a top-level element that pushes a child
// cursor; the empty row's child completes with zero elements. Guards the row-capture path
// that stores the top-level element's view (and not a nested child's) into each row.
TEST(TuringProtoRoundTripTest, RoundTripsListElementViewColumnOfNestedLists) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    // row 0 = [1, 2]
    std::vector<db::ListBuffer<>::ListItemVariant> firstItems;
    firstItems.emplace_back(Int64 {1});
    firstItems.emplace_back(Int64 {2});
    const db::ListView first = localMem.listBuffer().insert(firstItems);

    // row 1 = []
    std::vector<db::ListBuffer<>::ListItemVariant> emptyItems;
    const db::ListView empty = localMem.listBuffer().insert(emptyItems);

    // row 2 = ["only"]
    std::vector<db::ListBuffer<>::ListItemVariant> thirdItems;
    thirdItems.emplace_back(StringView {"only"});
    const db::ListView third = localMem.listBuffer().insert(thirdItems);

    // The column's rows are the three nested lists.
    std::vector<db::ListBuffer<>::ListItemVariant> rowItems;
    rowItems.emplace_back(first);
    rowItems.emplace_back(empty);
    rowItems.emplace_back(third);
    const db::ListView rows = localMem.listBuffer().insert(rowItems);

    auto* col = localMem.alloc<db::ColumnVector<db::ListElementView>>();
    for (const auto& element : rows) {
        col->push_back(element);
    }
    addColumn(&dfMan, &source, "rows", col);

    const auto packets = encodeDataframeWithChunkSize(source, 64);
    expectPacketSequence(packets, true);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedCol = decoded.cols().at(0)->as<db::ColumnVector<db::ListElementView>>();
    ASSERT_NE(decodedCol, nullptr);
    ASSERT_EQ(decodedCol->size(), 3u);

    EXPECT_EQ(decodedCol->at(0).getTag(), db::ListBufferTypeTag::ListView);
    const db::ListView firstView = decodedCol->at(0).getAs<db::ListView>();
    ASSERT_EQ(firstView.size(), 2u);
    EXPECT_EQ(firstView.begin()->getAs<Int64>(), 1);

    EXPECT_EQ(decodedCol->at(1).getTag(), db::ListBufferTypeTag::ListView);
    EXPECT_EQ(decodedCol->at(1).getAs<db::ListView>().size(), 0u);

    EXPECT_EQ(decodedCol->at(2).getTag(), db::ListBufferTypeTag::ListView);
    const db::ListView thirdView = decodedCol->at(2).getAs<db::ListView>();
    ASSERT_EQ(thirdView.size(), 1u);
    EXPECT_EQ(thirdView.begin()->getAs<StringView>(), std::string_view("only"));
}

// The schema (column names and types) must fit in a single CHUNK_HEADER
// packet; we never split a schema across packets. If the configured chunk
// size is too small to hold the schema, the encoder must throw at encode
// time rather than silently truncate or stall. Drives this with a chunk
// size known to be smaller than the schema for the two named columns.
TEST(TuringProtoRoundTripTest, RejectsSchemaLargerThanChunkSize) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    auto* ids = localMem.alloc<db::ColumnVector<UInt64>>();
    ids->push_back(1);
    addColumn(&dfMan, &source, "identifier", ids);

    auto* labels = localMem.alloc<db::ColumnOptVector<StringView>>();
    labels->push_back(std::optional<StringView> {std::string_view("value")});
    addColumn(&dfMan, &source, "longer_column_name", labels);

    EXPECT_THROW(encodeDataframeWithChunkSize(source, 24), TuringException);
}

// Encode a ColumnOptVector<ListElementView> — the shape an out-of-range list index reads
// as — and decode it back. A row that holds no element and a row that holds a null taken
// out of a list both travel as a null-tagged element, so what tells them apart on the
// far side is the column's null mask; the two must not collapse into one.
TEST(TuringProtoRoundTripTest, RoundTripsOptionalListElementViewColumns) {
    using OptionalElement = std::optional<db::ListElementView>;

    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    const std::string text(64, 'q');

    std::vector<db::ListBuffer<>::ListItemVariant> items;
    items.emplace_back(Int64 {-3});
    items.emplace_back(StringView {text});
    items.emplace_back(db::PropertyNull {});

    const db::ListView list = localMem.listBuffer().insert(items);

    std::vector<db::ListElementView> elements;
    for (const auto& element : list) {
        elements.push_back(element);
    }
    ASSERT_EQ(elements.size(), 3u);

    auto* col = localMem.alloc<db::ColumnOptVector<db::ListElementView>>();
    col->push_back(OptionalElement {elements[0]});
    col->push_back(std::nullopt);
    col->push_back(OptionalElement {elements[1]});
    col->push_back(OptionalElement {elements[2]});
    col->push_back(std::nullopt);
    addColumn(&dfMan, &source, "element", col);

    // Small enough to split the header, the mask and the string element's payload
    for (const size_t chunkSize : std::array<size_t, 4> {48, 64, 97, 256}) {
        SCOPED_TRACE(::testing::Message() << "chunkSize=" << chunkSize);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::MapBuffer<> mapBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 1u);
        const auto* decodedCol = decoded.cols().at(0)->as<db::ColumnOptVector<db::ListElementView>>();
        ASSERT_NE(decodedCol, nullptr);

        const std::vector<OptionalElement>& raw = decodedCol->getRaw();
        ASSERT_EQ(raw.size(), 5u);

        ASSERT_TRUE(raw[0].has_value());
        EXPECT_EQ(raw[0]->getTag(), db::ListBufferTypeTag::Int);
        EXPECT_EQ(raw[0]->getAs<Int64>(), -3);

        EXPECT_FALSE(raw[1].has_value());

        ASSERT_TRUE(raw[2].has_value());
        EXPECT_EQ(raw[2]->getTag(), db::ListBufferTypeTag::String);
        EXPECT_EQ(raw[2]->getAs<StringView>(), std::string_view(text));

        ASSERT_TRUE(raw[3].has_value());
        EXPECT_EQ(raw[3]->getTag(), db::ListBufferTypeTag::Null);

        EXPECT_FALSE(raw[4].has_value());
    }
}

// Encode a ColumnConst<std::optional<ListElementView>> — the shape an index read of a
// literal list at a literal position takes — both carrying an element and carrying none.
TEST(TuringProtoRoundTripTest, RoundTripsOptionalConstantListElementViewColumns) {
    using OptionalElement = std::optional<db::ListElementView>;

    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    const std::string text(64, 'z');

    std::vector<db::ListBuffer<>::ListItemVariant> items;
    items.emplace_back(StringView {text});

    const db::ListView list = localMem.listBuffer().insert(items);

    auto* element = localMem.alloc<db::ColumnConst<OptionalElement>>();
    element->set(OptionalElement {*list.begin()});
    addColumn(&dfMan, &source, "element", element);

    auto* missing = localMem.alloc<db::ColumnConst<OptionalElement>>();
    missing->set(std::nullopt);
    addColumn(&dfMan, &source, "missing", missing);

    // The element's payload outruns the buffer, so the decode of the constant resumes on a
    // later packet with its header already read
    const auto packets = encodeDataframeWithChunkSize(source, 48);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 2u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 2u);
    const auto* decodedElement = decoded.cols().at(0)->as<db::ColumnConst<OptionalElement>>();
    const auto* decodedMissing = decoded.cols().at(1)->as<db::ColumnConst<OptionalElement>>();
    ASSERT_NE(decodedElement, nullptr);
    ASSERT_NE(decodedMissing, nullptr);

    const OptionalElement decodedValue = decodedElement->at(0);
    ASSERT_TRUE(decodedValue.has_value());
    EXPECT_EQ(decodedValue->getTag(), db::ListBufferTypeTag::String);
    EXPECT_EQ(decodedValue->getAs<StringView>(), std::string_view(text));

    EXPECT_FALSE(decodedMissing->at(0).has_value());
}

// A projection of constants alone - MATCH (n) RETURN 5 - has no column whose length
// carries the row count, so the chunk states it outright. Encode one constant over three
// rows and check the decoded frame reports three, not the one value that crossed the wire.
TEST(TuringProtoRoundTripTest, RoundTripsConstantOnlyChunkRowCount) {
    constexpr size_t ROW_COUNT = 3;

    db::LocalMemory localMem;
    db::DataframeManager dfMan;

    auto* answer = localMem.alloc<db::ColumnConst<Int64>>();
    answer->set(5);

    const std::vector<std::string_view> names {"answer"};
    const std::vector<const db::Column*> columns {answer};

    net::proto::TuringProtoOutBuf schemaBuf(256);
    net::proto::TuringProtoOutBuf dataBuf(256);
    std::vector<FramedPacket> packets;

    {
        net::proto::TuringProtoEncoder encoder(&schemaBuf);
        encoder.writeColumnHeaders(names, columns);
        packets.push_back(FramedPacket {
            ._type = net::proto::MessageTypes::CHUNK_HEADER,
            ._bytes = framePacket(net::proto::MessageTypes::CHUNK_HEADER,
                                  std::string_view(schemaBuf.data(), schemaBuf.size()))});
    }

    net::proto::TuringProtoEncoder encoder(&dataBuf);
    encoder.writeColumns(columns, 0, ROW_COUNT);
    packets.push_back(FramedPacket {
        ._type = net::proto::MessageTypes::CHUNK,
        ._bytes = framePacket(net::proto::MessageTypes::CHUNK,
                              std::string_view(dataBuf.data(), dataBuf.size()))});
    dataBuf.reset();
    encoder.writeChunkFooter(ROW_COUNT);
    packets.push_back(FramedPacket {
        ._type = net::proto::MessageTypes::END_CHUNK,
        ._bytes = framePacket(net::proto::MessageTypes::END_CHUNK,
                              std::string_view(dataBuf.data(), dataBuf.size()))});

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    EXPECT_EQ(decoded.getLogicalRowCount(), ROW_COUNT);

    const auto* decodedAnswer = decoded.cols().at(0)->as<db::ColumnConst<Int64>>();
    ASSERT_NE(decodedAnswer, nullptr);
    EXPECT_EQ(decodedAnswer->at(0), 5);
}


// A list column followed by another column: any byte the list writes beyond its own
// elements is read back as the next column's data, so this pins that a list value is
// written exactly once. A single-column dataframe cannot catch that — the surplus is
// trailing and never read.
TEST(TuringProtoRoundTripTest, RoundTripsAListColumnFollowedByAnotherColumn) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    std::vector<db::ListBuffer<>::ListItemVariant> items;
    items.emplace_back(Int64 {1});
    items.emplace_back(Int64 {2});

    auto* listCol = localMem.alloc<db::ColumnConst<db::ListView>>();
    listCol->set(localMem.listBuffer().insert(items));
    addColumn(&dfMan, &source, "my_list", listCol);

    auto* tailCol = localMem.alloc<db::ColumnVector<UInt64>>();
    tailCol->push_back(7u);
    tailCol->push_back(8u);
    addColumn(&dfMan, &source, "tail", tailCol);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);
    expectPacketSequence(packets, true);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 2u);

    const auto* decodedList = decoded.cols().at(0)->as<db::ColumnConst<db::ListView>>();
    ASSERT_NE(decodedList, nullptr);
    EXPECT_EQ(decodedList->at(0).size(), 2u);

    const auto* decodedTail = decoded.cols().at(1)->as<db::ColumnVector<UInt64>>();
    ASSERT_NE(decodedTail, nullptr);
    ASSERT_EQ(decodedTail->size(), 2u);
    EXPECT_EQ(decodedTail->at(0), 7u);
    EXPECT_EQ(decodedTail->at(1), 8u);
}

// A constant map column, round-tripped: [entryCount][mapByteSize] then, per entry,
// [keyLen][keyBytes][tag][value] — with a nested map carrying its own header inline.
TEST(TuringProtoRoundTripTest, RoundTripsConstantMapColumn) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    std::vector<db::MapBuffer<>::MapKeyValuePair> inner;
    inner.emplace_back("d", Int64 {2});
    const db::MapView innerView = localMem.mapBuffer().insert(inner);

    std::vector<db::MapBuffer<>::MapKeyValuePair> outer;
    outer.emplace_back("a", Int64 {-7});
    outer.emplace_back("bb", StringView {"xyz"});
    outer.emplace_back("c", innerView);

    auto* mapCol = localMem.alloc<db::ColumnConst<db::MapView>>();
    mapCol->set(localMem.mapBuffer().insert(outer));
    addColumn(&dfMan, &source, "my_map", mapCol);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);
    expectPacketSequence(packets, true);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedMap = decoded.cols().at(0)->as<db::ColumnConst<db::MapView>>();
    ASSERT_NE(decodedMap, nullptr);

    const db::MapView view = decodedMap->at(0);
    ASSERT_EQ(view.size(), 3u);

    auto entry = view.begin();
    EXPECT_EQ(entry->getKey(), "a");
    EXPECT_EQ(entry->getValueTag(), db::MapBufferTypeTag::Int);
    EXPECT_EQ(entry->getValueAs<Int64>(), -7);
    ++entry;
    EXPECT_EQ(entry->getKey(), "bb");
    EXPECT_EQ(entry->getValueTag(), db::MapBufferTypeTag::String);
    EXPECT_EQ(entry->getValueAs<StringView>(), std::string_view("xyz"));
    ++entry;
    EXPECT_EQ(entry->getKey(), "c");
    ASSERT_EQ(entry->getValueTag(), db::MapBufferTypeTag::MapView);

    const db::MapView nested = entry->getValueAs<db::MapView>();
    ASSERT_EQ(nested.size(), 1u);
    EXPECT_EQ(nested.front().getKey(), "d");
    EXPECT_EQ(nested.front().getValueAs<Int64>(), 2);
}

// A map laid out row by row rather than held as the one value a constant is - what a cut
// over a map projection broadcasts it into. Each row carries its own map, so the decoder
// reads one [entryCount][mapByteSize] header per row rather than a single one per column.
TEST(TuringProtoRoundTripTest, RoundTripsMapVectorColumn) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    auto* mapCol = localMem.alloc<db::ColumnVector<db::MapView>>();

    constexpr size_t rowCount = 3;
    for (size_t row = 0; row < rowCount; row++) {
        std::vector<db::MapBuffer<>::MapKeyValuePair> entries;
        entries.emplace_back("index", Int64(static_cast<int64_t>(row)));
        entries.emplace_back("name", StringView {"row"});

        mapCol->push_back(localMem.mapBuffer().insert(entries));
    }

    addColumn(&dfMan, &source, "my_maps", mapCol);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);
    expectPacketSequence(packets, true);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedMaps = decoded.cols().at(0)->as<db::ColumnVector<db::MapView>>();
    ASSERT_NE(decodedMaps, nullptr);
    ASSERT_EQ(decodedMaps->size(), rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        const db::MapView view = decodedMaps->at(row);
        ASSERT_EQ(view.size(), 2u);

        auto entry = view.begin();
        EXPECT_EQ(entry->getKey(), "index");
        EXPECT_EQ(entry->getValueTag(), db::MapBufferTypeTag::Int);
        EXPECT_EQ(entry->getValueAs<Int64>(), static_cast<int64_t>(row));
        ++entry;
        EXPECT_EQ(entry->getKey(), "name");
        EXPECT_EQ(entry->getValueAs<StringView>(), std::string_view("row"));
    }
}

// The key is the one variable-length field a map entry carries before its value, so a tiny
// chunk size makes it straddle a packet boundary. Resuming relies on the key being handed to
// the sink before its bytes stream, so the map already expects a value and the resumed pass
// does not read the key length again.
TEST(TuringProtoRoundTripTest, RoundTripsMapKeysAndValuesAcrossMultipleBuffers) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    const std::string longKey(96, 'k');
    const std::string longValue(96, 'v');

    std::vector<db::MapBuffer<>::MapKeyValuePair> entries;
    entries.emplace_back(StringView {longKey}, Int64 {11});
    entries.emplace_back("short", StringView {longValue});

    auto* mapCol = localMem.alloc<db::ColumnConst<db::MapView>>();
    mapCol->set(localMem.mapBuffer().insert(entries));
    addColumn(&dfMan, &source, "my_map", mapCol);

    const auto packets = encodeDataframeWithChunkSize(source, 32);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 4u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedMap = decoded.cols().at(0)->as<db::ColumnConst<db::MapView>>();
    ASSERT_NE(decodedMap, nullptr);

    const db::MapView view = decodedMap->at(0);
    ASSERT_EQ(view.size(), 2u);

    auto entry = view.begin();
    EXPECT_EQ(entry->getKey(), std::string_view(longKey));
    EXPECT_EQ(entry->getValueAs<Int64>(), 11);
    ++entry;
    EXPECT_EQ(entry->getKey(), "short");
    EXPECT_EQ(entry->getValueAs<StringView>(), std::string_view(longValue));
}

// One map per row, with a chunk size small enough that maps split across packets, some of
// them mid-entry: the vector decoder must resume the open row rather than start a new one.
TEST(TuringProtoRoundTripTest, RoundTripsMapVectorColumnAcrossMultipleBuffers) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    constexpr size_t rowCount = 20;
    std::vector<std::string> names;
    names.reserve(rowCount);
    for (size_t row = 0; row < rowCount; row++) {
        names.push_back("name-" + std::to_string(row) + std::string(row * 3, 'x'));
    }

    auto* mapCol = localMem.alloc<db::ColumnVector<db::MapView>>();
    for (size_t row = 0; row < rowCount; row++) {
        std::vector<db::MapBuffer<>::MapKeyValuePair> entries;
        entries.emplace_back("age", Int64(static_cast<int64_t>(row)));
        entries.emplace_back("name", StringView {names[row]});
        entries.emplace_back("missing", db::PropertyNull {});
        entries.emplace_back("node", db::NodeID {row});
        entries.emplace_back("edge", db::EdgeID {row + 100});

        mapCol->push_back(localMem.mapBuffer().insert(entries));
    }

    addColumn(&dfMan, &source, "my_maps", mapCol);

    const auto packets = encodeDataframeWithChunkSize(source, 32);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 10u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedMaps = decoded.cols().at(0)->as<db::ColumnVector<db::MapView>>();
    ASSERT_NE(decodedMaps, nullptr);
    ASSERT_EQ(decodedMaps->size(), rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        const db::MapView view = decodedMaps->at(row);
        ASSERT_EQ(view.size(), 5u) << "row " << row;

        auto entry = view.begin();
        EXPECT_EQ(entry->getKey(), "age");
        EXPECT_EQ(entry->getValueAs<Int64>(), static_cast<int64_t>(row));
        ++entry;
        EXPECT_EQ(entry->getKey(), "name");
        EXPECT_EQ(entry->getValueAs<StringView>(), std::string_view(names[row]));
        ++entry;
        EXPECT_EQ(entry->getKey(), "missing");
        EXPECT_EQ(entry->getValueTag(), db::MapBufferTypeTag::Null);
        ++entry;
        EXPECT_EQ(entry->getKey(), "node");
        EXPECT_EQ(entry->getValueAs<db::NodeID>(), db::NodeID {row});
        ++entry;
        EXPECT_EQ(entry->getKey(), "edge");
        EXPECT_EQ(entry->getValueAs<db::EdgeID>(), db::EdgeID {row + 100});
    }
}

// A map holding a list, and a list nested inside that: the two container kinds interleave on
// one stack, so each level has to resume on the right one.
TEST(TuringProtoRoundTripTest, RoundTripsAMapHoldingNestedLists) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    std::vector<db::ListBuffer<>::ListItemVariant> innerItems;
    innerItems.emplace_back(Int64 {5});
    const db::ListView innerList = localMem.listBuffer().insert(innerItems);

    std::vector<db::ListBuffer<>::ListItemVariant> outerItems;
    outerItems.emplace_back(StringView {"deep"});
    outerItems.emplace_back(innerList);
    const db::ListView outerList = localMem.listBuffer().insert(outerItems);

    std::vector<db::MapBuffer<>::MapKeyValuePair> entries;
    entries.emplace_back("items", outerList);
    entries.emplace_back("n", Int64 {3});

    auto* mapCol = localMem.alloc<db::ColumnConst<db::MapView>>();
    mapCol->set(localMem.mapBuffer().insert(entries));
    addColumn(&dfMan, &source, "my_map", mapCol);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    const auto* decodedMap = decoded.cols().at(0)->as<db::ColumnConst<db::MapView>>();
    ASSERT_NE(decodedMap, nullptr);

    const db::MapView view = decodedMap->at(0);
    ASSERT_EQ(view.size(), 2u);

    auto entry = view.begin();
    EXPECT_EQ(entry->getKey(), "items");
    ASSERT_EQ(entry->getValueTag(), db::MapBufferTypeTag::ListView);

    const db::ListView decodedOuter = entry->getValueAs<db::ListView>();
    ASSERT_EQ(decodedOuter.size(), 2u);
    auto element = decodedOuter.begin();
    EXPECT_EQ(element->getAs<StringView>(), std::string_view("deep"));
    ++element;
    ASSERT_EQ(element->getTag(), db::ListBufferTypeTag::ListView);
    EXPECT_EQ(element->getAs<db::ListView>().size(), 1u);

    ++entry;
    EXPECT_EQ(entry->getKey(), "n");
    EXPECT_EQ(entry->getValueAs<Int64>(), 3);
}

// An empty map: [entryCount] of 0 and nothing after it.
TEST(TuringProtoRoundTripTest, RoundTripsAnEmptyMapColumn) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    const std::vector<db::MapBuffer<>::MapKeyValuePair> entries;

    auto* mapCol = localMem.alloc<db::ColumnConst<db::MapView>>();
    mapCol->set(localMem.mapBuffer().insert(entries));
    addColumn(&dfMan, &source, "my_map", mapCol);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    const auto* decodedMap = decoded.cols().at(0)->as<db::ColumnConst<db::MapView>>();
    ASSERT_NE(decodedMap, nullptr);
    EXPECT_TRUE(decodedMap->at(0).empty());
}

// One entry per mappable value tag. mapByteSize is summed from those tags on the encoder and
// spent on the decoder, so a tag the two size differently writes past the reserved region
// instead of failing where it is read.
TEST(TuringProtoRoundTripTest, RoundTripsAMapHoldingEveryValueTag) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    const std::vector<float> embedding {1.5F, -2.5F, 3.5F};

    std::vector<db::ListBuffer<>::ListItemVariant> listItems;
    listItems.emplace_back(Int64 {9});
    const db::ListView list = localMem.listBuffer().insert(listItems);

    std::vector<db::MapBuffer<>::MapKeyValuePair> nestedEntries;
    nestedEntries.emplace_back("deep", Bool {false});
    const db::MapView nested = localMem.mapBuffer().insert(nestedEntries);

    std::vector<db::MapBuffer<>::MapKeyValuePair> entries;
    entries.emplace_back("i", Int64 {-7});
    entries.emplace_back("u", UInt64 {7});
    entries.emplace_back("d", db::types::Double::Primitive {2.25});
    entries.emplace_back("s", StringView {"xyz"});
    entries.emplace_back("b", Bool {true});
    entries.emplace_back("e", Embedding {embedding});
    entries.emplace_back("l", list);
    entries.emplace_back("m", nested);
    entries.emplace_back("n", db::PropertyNull {});
    entries.emplace_back("node", db::NodeID {42});
    entries.emplace_back("edge", db::EdgeID {43});

    auto* mapCol = localMem.alloc<db::ColumnConst<db::MapView>>();
    mapCol->set(localMem.mapBuffer().insert(entries));
    addColumn(&dfMan, &source, "my_map", mapCol);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    const auto* decodedMap = decoded.cols().at(0)->as<db::ColumnConst<db::MapView>>();
    ASSERT_NE(decodedMap, nullptr);

    const db::MapView view = decodedMap->at(0);
    ASSERT_EQ(view.size(), 11u);

    const std::span<const db::MapEntryView> decodedEntries = view.entries();

    EXPECT_EQ(decodedEntries[0].getKey(), "i");
    EXPECT_EQ(decodedEntries[0].getValueAs<Int64>(), -7);

    EXPECT_EQ(decodedEntries[1].getKey(), "u");
    EXPECT_EQ(decodedEntries[1].getValueAs<UInt64>(), 7u);

    EXPECT_EQ(decodedEntries[2].getKey(), "d");
    EXPECT_EQ(decodedEntries[2].getValueAs<db::types::Double::Primitive>(), 2.25);

    EXPECT_EQ(decodedEntries[3].getKey(), "s");
    EXPECT_EQ(decodedEntries[3].getValueAs<StringView>(), std::string_view("xyz"));

    EXPECT_EQ(decodedEntries[4].getKey(), "b");
    EXPECT_TRUE(decodedEntries[4].getValueAs<Bool>());

    EXPECT_EQ(decodedEntries[5].getKey(), "e");
    ASSERT_EQ(decodedEntries[5].getValueTag(), db::MapBufferTypeTag::Embedding);
    expectEmbedding(decodedEntries[5].getValueAs<Embedding>(), std::span<const float>(embedding));

    EXPECT_EQ(decodedEntries[6].getKey(), "l");
    ASSERT_EQ(decodedEntries[6].getValueTag(), db::MapBufferTypeTag::ListView);
    EXPECT_EQ(decodedEntries[6].getValueAs<db::ListView>().front().getAs<Int64>(), 9);

    EXPECT_EQ(decodedEntries[7].getKey(), "m");
    ASSERT_EQ(decodedEntries[7].getValueTag(), db::MapBufferTypeTag::MapView);
    EXPECT_FALSE(decodedEntries[7].getValueAs<db::MapView>().front().getValueAs<Bool>());

    EXPECT_EQ(decodedEntries[8].getKey(), "n");
    EXPECT_EQ(decodedEntries[8].getValueTag(), db::MapBufferTypeTag::Null);

    EXPECT_EQ(decodedEntries[9].getKey(), "node");
    ASSERT_EQ(decodedEntries[9].getValueTag(), db::MapBufferTypeTag::NodeID);
    EXPECT_EQ(decodedEntries[9].getValueAs<db::NodeID>(), db::NodeID {42});

    EXPECT_EQ(decodedEntries[10].getKey(), "edge");
    ASSERT_EQ(decodedEntries[10].getValueTag(), db::MapBufferTypeTag::EdgeID);
    EXPECT_EQ(decodedEntries[10].getValueAs<db::EdgeID>(), db::EdgeID {43});
}

// The mirror of RoundTripsAMapHoldingNestedLists: a list may hold a map, so the wire has to
// carry one in that direction too. The encoder writes the map's entries inside the list's
// payload and the decoder rebuilds it as the MapView element the list stored.
TEST(TuringProtoRoundTripTest, RoundTripsAListHoldingAMap) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    std::vector<db::MapBuffer<>::MapKeyValuePair> entries;
    entries.emplace_back("age", Int64 {32});
    entries.emplace_back("name", StringView {"Remy"});
    const db::MapView inner = localMem.mapBuffer().insert(entries);

    std::vector<db::ListBuffer<>::ListItemVariant> outerItems;
    outerItems.emplace_back(Int64 {7});
    outerItems.emplace_back(inner);
    outerItems.emplace_back(StringView {"end"});

    auto* listCol = localMem.alloc<db::ColumnConst<db::ListView>>();
    listCol->set(localMem.listBuffer().insert(outerItems));
    addColumn(&dfMan, &source, "with_map", listCol);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    const auto* decodedList = decoded.cols().at(0)->as<db::ColumnConst<db::ListView>>();
    ASSERT_NE(decodedList, nullptr);

    const db::ListView view = decodedList->at(0);
    ASSERT_EQ(view.size(), 3u);

    auto element = view.begin();
    EXPECT_EQ(element->getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ(element->getAs<Int64>(), 7);

    ++element;
    ASSERT_EQ(element->getTag(), db::ListBufferTypeTag::MapView);

    const db::MapView decodedMap = element->getAs<db::MapView>();
    ASSERT_EQ(decodedMap.size(), 2u);

    auto entry = decodedMap.begin();
    EXPECT_EQ(entry->getKey(), "age");
    ASSERT_EQ(entry->getValueTag(), db::MapBufferTypeTag::Int);
    EXPECT_EQ(entry->getValueAs<Int64>(), 32);

    ++entry;
    EXPECT_EQ(entry->getKey(), "name");
    ASSERT_EQ(entry->getValueTag(), db::MapBufferTypeTag::String);
    EXPECT_EQ(entry->getValueAs<StringView>(), std::string_view("Remy"));

    ++element;
    EXPECT_EQ(element->getTag(), db::ListBufferTypeTag::String);
    EXPECT_EQ(element->getAs<StringView>(), std::string_view("end"));
}

// The row-capture sibling of RoundTripsAListHoldingAMap: each row of a ListElementView
// column is one top-level tagged element, so a map row has to record the element view the
// list cursor hands back. A constant list column never exercises that - its rows are the
// one list - so this is where a dropped view shows up.
TEST(TuringProtoRoundTripTest, RoundTripsListElementViewColumnOfMaps) {
    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    std::vector<db::MapBuffer<>::MapKeyValuePair> firstEntries;
    firstEntries.emplace_back("a", Int64 {1});

    std::vector<db::MapBuffer<>::MapKeyValuePair> secondEntries;
    secondEntries.emplace_back("b", Int64 {2});

    std::vector<db::ListBuffer<>::ListItemVariant> items;
    items.emplace_back(localMem.mapBuffer().insert(firstEntries));
    items.emplace_back(Int64 {7});
    items.emplace_back(localMem.mapBuffer().insert(secondEntries));

    const db::ListView list = localMem.listBuffer().insert(items);

    auto* col = localMem.alloc<db::ColumnVector<db::ListElementView>>();
    for (const auto& element : list) {
        col->push_back(element);
    }
    addColumn(&dfMan, &source, "elements", col);

    const auto packets = encodeDataframeWithChunkSize(source, 4096);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::MapBuffer<> mapBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &mapBuffer, &dfMan, &decoded, &schemas);

    const auto* decodedCol = decoded.cols().at(0)->as<db::ColumnVector<db::ListElementView>>();
    ASSERT_NE(decodedCol, nullptr);
    ASSERT_EQ(decodedCol->size(), 3u);

    ASSERT_EQ((*decodedCol)[0].getTag(), db::ListBufferTypeTag::MapView);
    const db::MapView first = (*decodedCol)[0].getAs<db::MapView>();
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first.front().getKey(), "a");
    EXPECT_EQ(first.front().getValueAs<Int64>(), 1);

    EXPECT_EQ((*decodedCol)[1].getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ((*decodedCol)[1].getAs<Int64>(), 7);

    ASSERT_EQ((*decodedCol)[2].getTag(), db::ListBufferTypeTag::MapView);
    const db::MapView second = (*decodedCol)[2].getAs<db::MapView>();
    ASSERT_EQ(second.size(), 1u);
    EXPECT_EQ(second.front().getKey(), "b");
    EXPECT_EQ(second.front().getValueAs<Int64>(), 2);
}
