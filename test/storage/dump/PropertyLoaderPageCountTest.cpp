#include <stdint.h>
#include <span>
#include <vector>

#include "TuringTest.h"

#include "dump/DumpConfig.h"
#include "dump/GraphDumpHelper.h"
#include "dump/PropertyContainerLoader.h"
#include "FilePageReader.h"
#include "FilePageWriter.h"

using namespace db;
using namespace turing::test;

namespace {

struct DumpPage {
    uint64_t _count {0};
    size_t _byteCount {0};
};

void writePropertyDump(const fs::Path& path,
                       ValueType valueType,
                       std::span<const uint64_t> metadata,
                       std::span<const DumpPage> pages) {
    auto writer = fs::FilePageWriter::open(path);
    ASSERT_TRUE(writer);
    fs::FilePageWriter& pageWriter = writer.value();

    GraphDumpHelper::writeFileHeader(pageWriter);
    pageWriter.writeToCurrentPage(valueType);
    for (const uint64_t field : metadata) {
        pageWriter.writeToCurrentPage(field);
    }

    for (const DumpPage& page : pages) {
        pageWriter.nextPage();
        pageWriter.writeToCurrentPage(page._count);

        const std::vector<uint8_t> payload(page._byteCount, 0);
        pageWriter.writeToCurrentPage(std::span<const uint8_t> {payload});
    }

    pageWriter.finish();
}

template <typename Loader>
bool loads(const fs::Path& path) {
    auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
    if (!reader) {
        return false;
    }

    Loader loader(reader.value());
    return loader.load().has_value();
}

}

class PropertyLoaderPageCountTest : public TuringTest {
};

TEST_F(PropertyLoaderPageCountTest, trivialIDPageOverflowsPropCount) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "ints";

    // propCount, idPageCount, valuePageCount, nullCount, nullPageCount
    const std::vector<uint64_t> metadata {1, 1, 0, 0, 0};
    const std::vector<DumpPage> pages {{2, 2 * sizeof(EntityID)}};
    writePropertyDump(path, ValueType::Int64, metadata, pages);

    EXPECT_FALSE(loads<TrivialPropertyContainerLoader<types::Int64>>(path));
}

TEST_F(PropertyLoaderPageCountTest, trivialPagesShortOfPropCount) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "ints";

    const std::vector<uint64_t> metadata {3, 1, 1, 0, 0};
    const std::vector<DumpPage> pages {{3, 3 * sizeof(EntityID)}, {1, sizeof(int64_t)}};
    writePropertyDump(path, ValueType::Int64, metadata, pages);

    EXPECT_FALSE(loads<TrivialPropertyContainerLoader<types::Int64>>(path));
}

TEST_F(PropertyLoaderPageCountTest, trivialPageCountBeyondPage) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "ints";

    const uint64_t countBeyondPage = DumpConfig::PAGE_SIZE;
    const std::vector<uint64_t> metadata {0, 0, 0, countBeyondPage, 1};
    const std::vector<DumpPage> pages {{countBeyondPage, 0}};
    writePropertyDump(path, ValueType::Int64, metadata, pages);

    EXPECT_FALSE(loads<TrivialPropertyContainerLoader<types::Int64>>(path));
}

TEST_F(PropertyLoaderPageCountTest, stringNullPageOverflowsNullCount) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "strings";

    // propCount, idPageCount, bucketPageCount, limitsPageCount, nullCount, nullPageCount
    const std::vector<uint64_t> metadata {0, 0, 0, 0, 1, 1};
    const std::vector<DumpPage> pages {{2, 2 * sizeof(EntityID)}};
    writePropertyDump(path, ValueType::String, metadata, pages);

    EXPECT_FALSE(loads<StringPropertyContainerLoader>(path));
}

TEST_F(PropertyLoaderPageCountTest, embeddingFloatPageOverflowsPropCount) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "embeddings";

    // propCount, dimension, idPageCount, floatPageCount, nullCount, nullPageCount
    const std::vector<uint64_t> metadata {1, 2, 1, 1, 0, 0};
    const std::vector<DumpPage> pages {{1, sizeof(EntityID)}, {4, 4 * sizeof(float)}};
    writePropertyDump(path, ValueType::Embedding, metadata, pages);

    EXPECT_FALSE(loads<EmbeddingPropertyContainerLoader>(path));
}

TEST_F(PropertyLoaderPageCountTest, listBytePageOverflowsTotalBytes) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "lists";

    // propCount, totalBytes, idPageCount, bytePageCount, nullCount, nullPageCount
    const std::vector<uint64_t> metadata {0, 4, 0, 1, 0, 0};
    const std::vector<DumpPage> pages {{16, 16}};
    writePropertyDump(path, ValueType::List, metadata, pages);

    EXPECT_FALSE(loads<ListPropertyContainerLoader>(path));
}

TEST_F(PropertyLoaderPageCountTest, mapBytePageOverflowsTotalBytes) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "maps";

    const std::vector<uint64_t> metadata {0, 4, 0, 1, 0, 0};
    const std::vector<DumpPage> pages {{16, 16}};
    writePropertyDump(path, ValueType::Map, metadata, pages);

    EXPECT_FALSE(loads<MapPropertyContainerLoader>(path));
}

TEST_F(PropertyLoaderPageCountTest, stringBucketPageCountBeyondPage) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "strings";

    const uint64_t countWrappingByteSize = uint64_t {1} << 46;
    const std::vector<uint64_t> metadata {0, 0, 1, 0, 0, 0};
    const std::vector<DumpPage> pages {{countWrappingByteSize, 0}};
    writePropertyDump(path, ValueType::String, metadata, pages);

    EXPECT_FALSE(loads<StringPropertyContainerLoader>(path));
}

TEST_F(PropertyLoaderPageCountTest, stringLimitBlocksBeyondPage) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "strings";

    const uint64_t blockCountBeyondPage = DumpConfig::PAGE_SIZE;
    const std::vector<uint64_t> metadata {0, 0, 1, 1, 0, 0};
    const std::vector<DumpPage> pages {{1, StringBucket::BUCKET_SIZE}, {blockCountBeyondPage, 0}};
    writePropertyDump(path, ValueType::String, metadata, pages);

    EXPECT_FALSE(loads<StringPropertyContainerLoader>(path));
}

TEST_F(PropertyLoaderPageCountTest, embeddingFloatCountWrapping) {
    const fs::Path path = fs::Path(_outDir.c_str()) / "embeddings";

    const uint64_t dimensionWrappingFloatCount = uint64_t {1} << 63;
    const std::vector<uint64_t> metadata {2, dimensionWrappingFloatCount, 1, 0, 0, 0};
    const std::vector<DumpPage> pages {{2, 2 * sizeof(EntityID)}};
    writePropertyDump(path, ValueType::Embedding, metadata, pages);

    EXPECT_FALSE(loads<EmbeddingPropertyContainerLoader>(path));
}
