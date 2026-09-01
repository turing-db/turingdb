#include <gtest/gtest.h>

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
#include "TuringProtoEncoder.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "LocalMemory.h"
#include "columns/ColumnConst.h"
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
using Embedding = db::types::Embedding::Primitive;

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
        encoder.writeDataframeHeader(&df);
        emitPacket(net::proto::MessageTypes::CHUNK_HEADER, &schemaBuf);
    }

    net::proto::TuringProtoEncoder encoder(&dataBuf);
    dataBuf.setOnBufferFullCallBack([&]() {
        emitPacket(net::proto::MessageTypes::CHUNK, &dataBuf);
    });

    encoder.writeDataframe(&df);
    if (dataBuf.size() > 0) {
        emitPacket(net::proto::MessageTypes::CHUNK, &dataBuf);
    }
    packets.push_back(FramedPacket {
        ._type = net::proto::MessageTypes::END_CHUNK,
        ._bytes = framePacket(net::proto::MessageTypes::END_CHUNK, {})});

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
                        db::DataframeManager* dfMan,
                        db::Dataframe* decoded,
                        std::vector<net::proto::DecodedColumnSchema>* schemas) {
    const size_t maxPayloadSize =
        std::transform_reduce(packets.begin(), packets.end(), size_t {0}, [](size_t lhs, size_t rhs) { return std::max(lhs, rhs); }, [](const FramedPacket& packet) { return packet._bytes.size() - net::proto::ProtoHeader::wireSize(); });
    net::proto::TuringProtoInBuf inBuf(maxPayloadSize);
    net::proto::TuringSink sink(localMem, embeddingBuffer, stringBuffer, listBuffer);
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
                EXPECT_EQ(protoHeader._dataLen, 0u);
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
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

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

// Optional string columns combine two harder cases: variable-length data
// (so length prefixes flow through the wire) and a null mask (so the
// presence bits must align with the values on decode). One of the strings
// is deliberately long enough that, at the smaller chunk sizes, it spans a
// CHUNK boundary mid-string. Verifies the decoder reassembles it correctly
// and preserves nulls in the right positions.
TEST(TuringProtoRoundTripTest, RoundTripsOptionalStringColumnsAcrossChunkSizes) {
    using OptionalDecodedString = std::optional<std::string>;
    using OptionalSourceString = std::optional<StringView>;
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
        labels->push_back(OptionalSourceString {"alpha"sv});
        labels->push_back(std::nullopt);
        labels->push_back(OptionalSourceString {"this string is deliberately long enough to cross chunk boundaries"sv});
        labels->push_back(OptionalSourceString {"omega"sv});
        addColumn(&dfMan, &source, "label", labels);

        const auto packets = encodeDataframeWithChunkSize(source, chunkSize);
        expectPacketSequence(packets, true);

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<char> stringBuffer;
        db::ListBuffer<> listBuffer;
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 2u);
        EXPECT_EQ(decoded.getLogicalRowCount(), 4u);

        const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnVector<UInt64>>();
        const auto* decodedLabels = decoded.cols().at(1)->as<db::ColumnVector<OptionalDecodedString>>();
        ASSERT_NE(decodedIds, nullptr);
        ASSERT_NE(decodedLabels, nullptr);

        EXPECT_EQ(decodedIds->getRaw(),
                  (std::vector<UInt64> {101, 102, 103, 104}));
        EXPECT_EQ(decodedLabels->getRaw(),
                  (std::vector<OptionalDecodedString> {
                      std::string("alpha"),
                      std::nullopt,
                      std::string("this string is deliberately long enough to cross chunk boundaries"),
                      std::string("omega")}));
    }
}

// Stress the chunked-string path with a single value that is several times
// the chunk size, so it cannot live in one CHUNK packet and the encoder
// must split it. Asserts that at least 5 CHUNK packets were emitted (proves
// the split actually happened) and that the full string round-trips to the
// decoder side with no loss.
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
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

        ASSERT_EQ(decoded.cols().size(), 2u);
        const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnVector<UInt64>>();
        const auto* decodedLabels = decoded.cols().at(1)->as<db::ColumnVector<std::string>>();
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
        db::Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;
        decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

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
    using OptionalDecodedString = std::optional<std::string>;
    using OptionalSourceString = std::optional<StringView>;

    db::LocalMemory localMem;
    db::DataframeManager dfMan;
    db::Dataframe source;

    auto* maybeId = localMem.alloc<db::ColumnConst<std::optional<UInt64>>>();
    maybeId->set(std::optional<UInt64> {42});
    addColumn(&dfMan, &source, "maybe_id", maybeId);

    const std::string hugeLabel(512, 'q');
    auto* maybeLabel = localMem.alloc<db::ColumnConst<OptionalSourceString>>();
    maybeLabel->set(OptionalSourceString {std::string_view(hugeLabel)});
    addColumn(&dfMan, &source, "maybe_label", maybeLabel);

    auto* emptyLabel = localMem.alloc<db::ColumnConst<OptionalSourceString>>();
    emptyLabel->set(std::nullopt);
    addColumn(&dfMan, &source, "empty_label", emptyLabel);

    const auto packets = encodeDataframeWithChunkSize(source, 96);
    expectPacketSequence(packets, true);
    EXPECT_GE(countPacketsOfType(packets, net::proto::MessageTypes::CHUNK), 2u);

    net::proto::ChunkedBuffer<float> embeddingBuffer;
    net::proto::ChunkedBuffer<char> stringBuffer;
    db::ListBuffer<> listBuffer;
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 3u);
    const auto* decodedIds = decoded.cols().at(0)->as<db::ColumnConst<std::optional<UInt64>>>();
    const auto* decodedLabel = decoded.cols().at(1)->as<db::ColumnConst<OptionalDecodedString>>();
    const auto* decodedEmptyLabel = decoded.cols().at(2)->as<db::ColumnConst<OptionalDecodedString>>();
    ASSERT_NE(decodedIds, nullptr);
    ASSERT_NE(decodedLabel, nullptr);
    ASSERT_NE(decodedEmptyLabel, nullptr);

    EXPECT_EQ(decodedIds->at(0), std::optional<UInt64> {42});
    EXPECT_EQ(decodedLabel->at(0), OptionalDecodedString {hugeLabel});
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
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

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
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

    ASSERT_EQ(decoded.cols().size(), 1u);
    const auto* decodedCol = decoded.cols().at(0)->as<db::ColumnVector<db::ListElementView>>();
    ASSERT_NE(decodedCol, nullptr);
    ASSERT_EQ(decodedCol->size(), 4u);

    EXPECT_EQ(decodedCol->at(0).getTag(), db::ListBufferTypeTag::Int);
    EXPECT_EQ(decodedCol->at(0).getAs<Int64>(), -3);
    EXPECT_EQ(decodedCol->at(1).getTag(), db::ListBufferTypeTag::String);
    EXPECT_EQ(decodedCol->at(1).getAs<StringView>(), std::string_view(text));
    EXPECT_EQ(decodedCol->at(2).getTag(), db::ListBufferTypeTag::Embedding);
    expectEmbedding(decodedCol->at(2).getAs<Embedding>(), std::span<const float>(embedding));
    EXPECT_EQ(decodedCol->at(3).getTag(), db::ListBufferTypeTag::UInt);
    EXPECT_EQ(decodedCol->at(3).getAs<UInt64>(), 123u);
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
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

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
    db::Dataframe decoded;
    std::vector<net::proto::DecodedColumnSchema> schemas;
    decodeChunkPackets(packets, &localMem, &embeddingBuffer, &stringBuffer, &listBuffer, &dfMan, &decoded, &schemas);

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
