#include "TuringTest.h"

#include "dump/DumpConfig.h"
#include "dump/PropertyContainerDumper.h"
#include "dump/PropertyContainerLoader.h"
#include "comparators/PropertyContainerComparator.h"
#include "embedding/EmbeddingBucket.h"
#include "FilePageReader.h"
#include "FilePageWriter.h"

using namespace db;
using namespace turing::test;

class PropertyContainerDumperTest : public TuringTest {
    void initialize() override {
    }

    void terminate() override {
    }
};

TEST_F(PropertyContainerDumperTest, emptyStrings) {
    fs::Path outDir(_outDir.c_str());

    auto writer = fs::FilePageWriter::open(outDir / "strings");
    ASSERT_TRUE(writer);

    StringPropertyContainerDumper dumper(writer.value());

    TypedPropertyContainer<types::String> container;
    ASSERT_TRUE(dumper.dump(container));
}

TEST_F(PropertyContainerDumperTest, emptyInts) {
    fs::Path outDir(_outDir.c_str());

    auto writer = fs::FilePageWriter::open(outDir / "ints");
    ASSERT_TRUE(writer);

    TrivialPropertyContainerDumper<types::Int64> dumper(writer.value());

    TypedPropertyContainer<types::Int64> container;
    ASSERT_TRUE(dumper.dump(container));
}

TEST_F(PropertyContainerDumperTest, manyStrings) {
    fs::Path outDir(_outDir.c_str());

    auto writer = fs::FilePageWriter::open(outDir / "strings");
    ASSERT_TRUE(writer);

    StringPropertyContainerDumper dumper(writer.value());

    TypedPropertyContainer<types::String> container;

    static constexpr std::string_view str = "Hello, world!";
    for (EntityID id = 0; id < 100'000; id++) {
        container.add(id, str);
    }

    ASSERT_TRUE(dumper.dump(container));
}

TEST_F(PropertyContainerDumperTest, manyInts) {
    fs::Path outDir(_outDir.c_str());

    auto writer = fs::FilePageWriter::open(outDir / "ints");
    ASSERT_TRUE(writer);

    TrivialPropertyContainerDumper<types::Int64> dumper(writer.value());

    TypedPropertyContainer<types::Int64> container;

    for (EntityID id = 0; id < 1000'000; id++) {
        container.add(id, (types::Int64::Primitive)id.getValue());
    }

    ASSERT_TRUE(dumper.dump(container));
}

// A container is dumped in its own order, so a loader reads the IDs back sorted whenever
// the dumped ones were. The flag has to come back with them: it is what picks the
// galloping override drop in the property value scan.
TEST_F(PropertyContainerDumperTest, sortedIntsAreLoadedAsSorted) {
    fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "sorted_ints";

    TypedPropertyContainer<types::Int64> original;
    for (EntityID id = 0; id < 1000; id++) {
        original.add(id, static_cast<types::Int64::Primitive>(id.getValue()));
    }
    original.sort();
    ASSERT_TRUE(original.isSorted());

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        TrivialPropertyContainerDumper<types::Int64> dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
    ASSERT_TRUE(reader);
    TrivialPropertyContainerLoader<types::Int64> loader(reader.value());
    auto result = loader.load();
    ASSERT_TRUE(result);

    const auto& loaded = result.value()->cast<types::Int64>();
    EXPECT_TRUE(loaded.isSorted());
    EXPECT_TRUE(PropertyContainerComparator::same(&original, &loaded));
}

TEST_F(PropertyContainerDumperTest, unsortedIntsAreLoadedAsUnsorted) {
    fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "unsorted_ints";

    TypedPropertyContainer<types::Int64> original;
    for (size_t index = 1000; index > 0; index--) {
        original.add(EntityID(index - 1), static_cast<types::Int64::Primitive>(index));
    }
    ASSERT_FALSE(original.isSorted());

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        TrivialPropertyContainerDumper<types::Int64> dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
    ASSERT_TRUE(reader);
    TrivialPropertyContainerLoader<types::Int64> loader(reader.value());
    auto result = loader.load();
    ASSERT_TRUE(result);

    EXPECT_FALSE(result.value()->isSorted());
}

TEST_F(PropertyContainerDumperTest, sortedStringsAreLoadedAsSorted) {
    fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "sorted_strings";

    TypedPropertyContainer<types::String> original;
    for (EntityID id = 0; id < 1000; id++) {
        const std::string value = "s" + std::to_string(id.getValue());
        original.add(id, value);
    }
    original.sort();
    ASSERT_TRUE(original.isSorted());

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        StringPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
    ASSERT_TRUE(reader);
    StringPropertyContainerLoader loader(reader.value());
    auto result = loader.load();
    ASSERT_TRUE(result);

    const auto& loaded = result.value()->cast<types::String>();
    EXPECT_TRUE(loaded.isSorted());
    EXPECT_TRUE(PropertyContainerComparator::same(&original, &loaded));
}

TEST_F(PropertyContainerDumperTest, sortedEmbeddingsAreLoadedAsSorted) {
    fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "sorted_embeddings";
    const size_t dimension = 4;

    TypedPropertyContainer<types::Embedding> original(dimension);

    std::vector<float> embedding(dimension);
    for (EntityID id = 0; id < 100; id++) {
        for (size_t d = 0; d < dimension; d++) {
            embedding[d] = static_cast<float>(id.getValue() * dimension + d);
        }
        original.add(id, embedding);
    }
    original.sort();
    ASSERT_TRUE(original.isSorted());

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        EmbeddingPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

    auto reader = fs::FilePageReader::open(path, DumpConfig::PAGE_SIZE);
    ASSERT_TRUE(reader);
    EmbeddingPropertyContainerLoader loader(reader.value());
    auto result = loader.load();
    ASSERT_TRUE(result);

    const auto& loaded = result.value()->cast<types::Embedding>();
    EXPECT_TRUE(loaded.isSorted());
    EXPECT_TRUE(PropertyContainerComparator::same(&original, &loaded));
}

TEST_F(PropertyContainerDumperTest, emptyEmbeddingRoundTrip) {
    fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "embeddings_empty";

    TypedPropertyContainer<types::Embedding> original(3);

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        EmbeddingPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

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

TEST_F(PropertyContainerDumperTest, embeddingRoundTrip) {
    fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "embeddings";
    const size_t dimension = 4;

    TypedPropertyContainer<types::Embedding> original(dimension);

    std::vector<float> embedding(dimension);
    for (EntityID id = 0; id < 1000; id++) {
        for (size_t d = 0; d < dimension; d++) {
            embedding[d] = static_cast<float>(id.getValue() * dimension + d);
        }
        original.add(id, embedding);
    }

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        EmbeddingPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

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

TEST_F(PropertyContainerDumperTest, manyEmbeddingsRoundTrip) {
    fs::Path outDir(_outDir.c_str());
    const fs::Path path = outDir / "embeddings_many";
    const size_t dimension = 8;

    EmbeddingBucket probe(dimension);
    const size_t count = probe.getAvailCount() * 2 + 100;

    TypedPropertyContainer<types::Embedding> original(dimension);

    std::vector<float> embedding(dimension);
    for (size_t i = 0; i < count; i++) {
        for (size_t d = 0; d < dimension; d++) {
            embedding[d] = static_cast<float>(i * dimension + d);
        }
        original.add(EntityID(i), embedding);
    }

    {
        auto writer = fs::FilePageWriter::open(path);
        ASSERT_TRUE(writer);
        EmbeddingPropertyContainerDumper dumper(writer.value());
        ASSERT_TRUE(dumper.dump(original));
    }

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
