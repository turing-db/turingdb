#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ChunkedBuffer.h"
#include "GraphPath.h"
#include "LocalMemory.h"
#include "TuringException.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "TuringSink.h"
#include "TuringSinkColumnContainer.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

using StringView = types::String::Primitive;
using Embedding = types::Embedding::Primitive;

constexpr size_t BUFFER_CAPACITY = 4096;
constexpr size_t MISSING_BYTE_COUNT = 8;

void loadPayload(std::string_view payload, net::proto::TuringProtoInBuf& inBuf) {
    inBuf.reset();
    memcpy(inBuf.data(), payload.data(), payload.size());
    inBuf.increaseWriteOffset(payload.size());
}

}

// A CHUNK packet that ends inside its last value leaves the column without its bytes, so the
// chunk footer that follows rejects the chunk
class ChunkMissingBytesTest : public ::testing::Test {
protected:
    template <typename ColumnT>
    void addColumn(std::string_view name, ColumnT* column) {
        NamedColumn* namedColumn = NamedColumn::create(&_dataframeManager, column, _dataframeManager.allocTag());
        namedColumn->rename(name);
        _source.addColumn(namedColumn);
    }

    void expectTruncatedChunkRejected() {
        std::vector<std::string_view> names;
        std::vector<const Column*> columns;
        for (const NamedColumn* namedColumn : _source.cols()) {
            names.push_back(namedColumn->getName());
            columns.push_back(namedColumn->getColumn());
        }

        net::proto::TuringProtoOutBuf outBuf(BUFFER_CAPACITY);
        outBuf.setOnBufferFullCallBack([]() {
            ADD_FAILURE() << "the values do not fit one packet";
        });
        net::proto::TuringProtoEncoder encoder(&outBuf);

        encoder.writeColumnHeaders(names, columns);
        const std::string header(outBuf.data(), outBuf.size());
        outBuf.reset();

        const size_t rowCount = _source.getLogicalRowCount();
        encoder.writeColumns(columns, 0, rowCount);
        ASSERT_GT(outBuf.size(), MISSING_BYTE_COUNT);
        const std::string truncatedChunk(outBuf.data(), outBuf.size() - MISSING_BYTE_COUNT);
        outBuf.reset();

        encoder.writeChunkFooter(rowCount);
        const std::string footer(outBuf.data(), outBuf.size());

        net::proto::ChunkedBuffer<float> embeddingBuffer;
        net::proto::ChunkedBuffer<uint64_t> pathBuffer;
        ListBuffer<> listBuffer;
        MapBuffer<> mapBuffer;
        DataframeManager decodedManager;
        Dataframe decoded;
        std::vector<net::proto::DecodedColumnSchema> schemas;

        net::proto::TuringProtoInBuf inBuf(BUFFER_CAPACITY);
        net::proto::TuringSink sink(&_memory, &embeddingBuffer, &pathBuffer, &listBuffer, &mapBuffer);
        net::proto::TuringSinkColumnContainer container(&decoded, &decodedManager);
        net::proto::TuringProtoDecoder<net::proto::TuringSink> decoder(&inBuf, &sink, schemas);

        loadPayload(header, inBuf);
        decoder.decodeIncomingChunkHeader(&container);

        loadPayload(truncatedChunk, inBuf);
        decoder.decodeIncomingChunk(&container);

        loadPayload(footer, inBuf);
        EXPECT_THROW(decoder.decodeChunkFooter(&container), TuringException);
    }

    LocalMemory _memory;
    DataframeManager _dataframeManager;
    Dataframe _source;
};

TEST_F(ChunkMissingBytesTest, rejectsAConstantStringMissingBytes) {
    auto* name = _memory.alloc<ColumnConst<StringView>>();
    name->set(StringView("a constant string longer than the bytes the chunk loses"));
    addColumn("name", name);

    expectTruncatedChunkRejected();
}

TEST_F(ChunkMissingBytesTest, rejectsAConstantEmbeddingMissingBytes) {
    const std::vector<float> values {0.5f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f};

    auto* embedding = _memory.alloc<ColumnConst<Embedding>>();
    embedding->set(Embedding(std::span<const float>(values)));
    addColumn("embedding", embedding);

    expectTruncatedChunkRejected();
}

TEST_F(ChunkMissingBytesTest, rejectsAConstantPathMissingBytes) {
    GraphPath route;
    route.start(NodeID(10));
    route.addHop(EdgeID(20), NodeID(11));
    route.addHop(EdgeID(21), NodeID(12));

    auto* path = _memory.alloc<ColumnConst<GraphPath>>();
    path->set(route);
    addColumn("path", path);

    expectTruncatedChunkRejected();
}

TEST_F(ChunkMissingBytesTest, rejectsAStringColumnMissingBytesOfItsLastRow) {
    auto* names = _memory.alloc<ColumnVector<StringView>>();
    names->push_back(StringView("first row"));
    names->push_back(StringView("the last row, longer than the bytes the chunk loses"));
    addColumn("names", names);

    expectTruncatedChunkRejected();
}
