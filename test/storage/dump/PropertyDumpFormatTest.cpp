#include "TuringTest.h"

#include "dump/DumpConfig.h"
#include "dump/PropertyContainerDumper.h"
#include "dump/PropertyContainerLoader.h"
#include "comparators/PropertyContainerComparator.h"
#include "embedding/EmbeddingBucket.h"
#include "File.h"
#include "FileReader.h"
#include "FilePageReader.h"
#include "FilePageWriter.h"

using namespace db;
using namespace turing::test;

// These tests pin the on-disk property dump byte format with content hashes.
// The expected hashes were produced by the pre-bulk-write dumpers, which
// wrote IDs, string limits and embedding floats one element at a time, so a
// failure here means the dump byte format changed: either an accidental
// byte-stream regression in the dumpers, or a deliberate format change — in
// which case bump DumpConfig::VERSION and regenerate the hashes.
//
// Each test also loads the dump back and compares contents, so together the
// hash and the round trip prove that dumps written by older builds stay
// loadable by the current loader.
class PropertyDumpFormatTest : public TuringTest {
public:
    static constexpr uint64_t FNV_OFFSET_BASIS = 1469598103934665603ull;
    static constexpr uint64_t FNV_PRIME = 1099511628211ull;

    void hashFile(const fs::Path& path, uint64_t& hash) {
        auto file = fs::File::open(path);
        ASSERT_TRUE(file);

        fs::FileReader reader;
        reader.setFile(&file.value());
        reader.read();
        ASSERT_FALSE(reader.errorOccured());

        uint64_t value = FNV_OFFSET_BASIS;
        for (const uint8_t byte : reader.getBuffer()) {
            value = (value ^ byte) * FNV_PRIME;
        }

        hash = value;
    }
};

TEST_F(PropertyDumpFormatTest, int64DumpBytes) {
    const fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "ints";

    // 200'000 values span two ID pages and two value pages.
    TypedPropertyContainer<types::Int64> original;
    for (size_t i = 0; i < 200'000; i++) {
        original.add(EntityID(i), static_cast<types::Int64::Primitive>(i * 3 + 7));
    }

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        TrivialPropertyContainerDumper<types::Int64> dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    uint64_t hash = 0;
    hashFile(path, hash);
    EXPECT_EQ(hash, 13083236007965161758ull);

    {
        auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
        ASSERT_TRUE(reader);
        TrivialPropertyContainerLoader<types::Int64> loader(reader.value());
        auto result = loader.load();
        ASSERT_TRUE(result);

        const auto& loaded = result.value()->cast<types::Int64>();
        ASSERT_TRUE(PropertyContainerComparator::same(&original, &loaded));
    }
}

TEST_F(PropertyDumpFormatTest, stringDumpBytes) {
    const fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "strings";

    // 132'072 four-character strings fill two 256KB buckets exactly and start
    // a third, producing enough limit entries to split a limits block across
    // pages — the format's worst-case branch.
    TypedPropertyContainer<types::String> original;

    std::string content(4, 'a');
    for (size_t i = 0; i < 132'072; i++) {
        content[0] = static_cast<char>('a' + (i % 26));
        content[1] = static_cast<char>('a' + ((i / 26) % 26));
        content[2] = static_cast<char>('a' + ((i / 676) % 26));
        content[3] = static_cast<char>('a' + ((i / 17'576) % 26));
        original.add(EntityID(i), content);
    }

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        StringPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    uint64_t hash = 0;
    hashFile(path, hash);
    EXPECT_EQ(hash, 13294265124095545638ull);

    {
        auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
        ASSERT_TRUE(reader);
        StringPropertyContainerLoader loader(reader.value());
        auto result = loader.load();
        ASSERT_TRUE(result);

        const auto& loaded = result.value()->cast<types::String>();
        ASSERT_TRUE(PropertyContainerComparator::same(&original, &loaded));
    }
}

TEST_F(PropertyDumpFormatTest, embeddingDumpBytes) {
    const fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "embeddings";

    // Dimension 7 does not divide the per-page float count, so pages break
    // mid-embedding; the count spans three buckets, so contiguous runs break
    // at bucket boundaries.
    const size_t dimension = 7;

    EmbeddingBucket probe(dimension);
    const size_t count = probe.getAvailCount() * 2 + 100;

    TypedPropertyContainer<types::Embedding> original(dimension);

    std::vector<float> embedding(dimension);
    for (size_t i = 0; i < count; i++) {
        for (size_t j = 0; j < dimension; j++) {
            embedding[j] = static_cast<float>(i * dimension + j);
        }
        original.add(EntityID(i), embedding);
    }

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        EmbeddingPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    uint64_t hash = 0;
    hashFile(path, hash);
    EXPECT_EQ(hash, 14912157516766626100ull);

    {
        auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
        ASSERT_TRUE(reader);
        EmbeddingPropertyContainerLoader loader(reader.value());
        auto result = loader.load();
        ASSERT_TRUE(result);

        const auto& loaded = result.value()->cast<types::Embedding>();
        ASSERT_TRUE(PropertyContainerComparator::same(&original, &loaded));
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
