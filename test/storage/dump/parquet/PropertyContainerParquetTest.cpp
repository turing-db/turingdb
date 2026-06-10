#include "TuringTest.h"

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "dump/parquet/PropertyContainerParquetDumper.h"
#include "dump/parquet/PropertyContainerParquetLoader.h"
#include "dump/parquet/PropertyContainerParquetLayout.h"
#include "comparators/PropertyContainerComparator.h"
#include "properties/PropertyContainer.h"
#include "metadata/PropertyType.h"

#include <stdint.h>

#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "Path.h"
#include "FatalException.h"

using namespace db;
using namespace turing::test;

class PropertyContainerParquetTest : public TuringTest {
protected:
    void initialize() override {
    }

    void roundTrip(const PropertyContainer& original, const char* fileName) {
        const fs::Path path = fs::Path(_outDir) / fileName;
        PropertyContainerParquetDumper::dump(original, path);
        const std::unique_ptr<PropertyContainer> loaded =
            PropertyContainerParquetLoader::load(path);
        EXPECT_TRUE(PropertyContainerComparator::same(&original, loaded.get()));
    }
};

TEST_F(PropertyContainerParquetTest, Int64RoundTrip) {
    TypedPropertyContainer<types::Int64> original;
    for (uint64_t i = 0; i < 300; ++i) {
        original.add(EntityID {i * 3}, static_cast<int64_t>(i) - 150);
    }
    roundTrip(original, "int64.parquet");
}

TEST_F(PropertyContainerParquetTest, UInt64BoundaryRoundTrip) {
    TypedPropertyContainer<types::UInt64> original;
    const std::vector<uint64_t> values {
        0u,
        1u,
        42u,
        (static_cast<uint64_t>(1) << 63),
        std::numeric_limits<uint64_t>::max()};
    for (size_t i = 0; i < values.size(); ++i) {
        original.add(EntityID {static_cast<uint64_t>(i)}, values[i]);
    }
    roundTrip(original, "uint64.parquet");
}

TEST_F(PropertyContainerParquetTest, DoubleRoundTrip) {
    TypedPropertyContainer<types::Double> original;
    for (uint64_t i = 0; i < 200; ++i) {
        original.add(EntityID {i}, static_cast<double>(i) * 0.5 - 25.0);
    }
    roundTrip(original, "double.parquet");
}

TEST_F(PropertyContainerParquetTest, BoolRoundTrip) {
    TypedPropertyContainer<types::Bool> original;
    for (uint64_t i = 0; i < 64; ++i) {
        original.add(EntityID {i}, (i % 3) == 0);
    }
    roundTrip(original, "bool.parquet");
}

TEST_F(PropertyContainerParquetTest, StringRoundTrip) {
    TypedPropertyContainer<types::String> original;
    const std::vector<std::string> strings {
        "alice", "", "bob", "a-much-longer-string-value", "x"};
    for (size_t i = 0; i < strings.size(); ++i) {
        original.add(EntityID {static_cast<uint64_t>(i)}, std::string_view(strings[i]));
    }
    roundTrip(original, "string.parquet");
}

TEST_F(PropertyContainerParquetTest, EmbeddingRoundTrip) {
    const size_t dimension = 4;
    TypedPropertyContainer<types::Embedding> original(dimension);
    std::vector<float> embedding(dimension);
    for (uint64_t i = 0; i < 100; ++i) {
        for (size_t d = 0; d < dimension; ++d) {
            embedding[d] = static_cast<float>(i * dimension + d);
        }
        original.add(EntityID {i}, std::span<const float>(embedding.data(), dimension));
    }
    roundTrip(original, "embedding.parquet");
}

TEST_F(PropertyContainerParquetTest, MissingEmbeddingDimensionThrows) {
    namespace layout = propertyContainerParquetLayout;

    // An embedding-typed file without the dimension metadata cannot be interpreted.
    const fs::Path path = fs::Path(_outDir) / "bad-embedding.parquet";
    {
        ParquetWriteSchema schema;
        schema.addColumn(layout::ENTITY_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addFixedLenColumn(layout::VALUE_COLUMN, 4 * sizeof(float));

        ParquetWriter writer(path, schema);
        writer.setMetadata(layout::VALUE_TYPE_KEY,
                           std::to_string(static_cast<unsigned>(ValueType::Embedding)));
        writer.finish();
    }

    EXPECT_THROW(PropertyContainerParquetLoader::load(path), FatalException);
}

TEST_F(PropertyContainerParquetTest, ValueColumnTypeMismatchThrows) {
    namespace layout = propertyContainerParquetLayout;

    // The metadata claims Double but the value column holds INT64; the loader must
    // reject the file rather than silently dropping the column.
    const fs::Path path = fs::Path(_outDir) / "bad-double.parquet";
    {
        ParquetWriteSchema schema;
        schema.addColumn(layout::ENTITY_ID_COLUMN, ParquetColumnType::UInt64);
        schema.addColumn(layout::VALUE_COLUMN, ParquetColumnType::Int64);

        ParquetWriter writer(path, schema);
        writer.setMetadata(layout::VALUE_TYPE_KEY,
                           std::to_string(static_cast<unsigned>(ValueType::Double)));
        writer.finish();
    }

    EXPECT_THROW(PropertyContainerParquetLoader::load(path), FatalException);
}
