#include "TuringTest.h"

#include "dump/parquet/GraphMetadataParquetDumper.h"
#include "dump/parquet/GraphMetadataParquetLoader.h"
#include "comparators/GraphMetadataComparator.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "writers/MetadataBuilder.h"

#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class GraphMetadataParquetTest : public TuringTest {
protected:
    void initialize() override {
    }
};

TEST_F(GraphMetadataParquetTest, RoundTrip) {
    GraphMetadata original;
    GraphMetadata previous;
    const auto builder = MetadataBuilder::create(previous, &original);

    builder->getOrCreateLabel("Protein");
    builder->getOrCreateLabel("Gene");
    builder->getOrCreateLabel("Disease");

    builder->getOrCreateEdgeType("Interacts");
    builder->getOrCreateEdgeType("IsA");

    // One of every value type — value_type must round-trip alongside the name.
    builder->getOrCreatePropertyType("name", ValueType::String);
    builder->getOrCreatePropertyType("score", ValueType::Double);
    builder->getOrCreatePropertyType("count", ValueType::Int64);
    builder->getOrCreatePropertyType("flag", ValueType::Bool);
    builder->getOrCreatePropertyType("embedding", ValueType::Embedding);

    // Label sets spanning multiple backing integers and the sign bit, to exercise the
    // full-range uint64 round-trip of every integer column.
    builder->getOrCreateLabelSet(LabelSet::fromList({LabelID {0}}));
    builder->getOrCreateLabelSet(LabelSet::fromList({LabelID {0}, LabelID {1}}));
    builder->getOrCreateLabelSet(LabelSet::fromList({LabelID {63}}));
    builder->getOrCreateLabelSet(LabelSet::fromList({LabelID {64}}));
    builder->getOrCreateLabelSet(LabelSet::fromList({LabelID {200}}));

    const fs::Path commitDir = fs::Path(_outDir);
    GraphMetadataParquetDumper::dump(original, commitDir);

    GraphMetadata loaded;
    GraphMetadataParquetLoader::load(commitDir, loaded);

    EXPECT_TRUE(GraphMetadataComparator::same(original, loaded));
}

TEST_F(GraphMetadataParquetTest, EmptyRoundTrip) {
    const GraphMetadata original;

    const fs::Path commitDir = fs::Path(_outDir);
    GraphMetadataParquetDumper::dump(original, commitDir);

    GraphMetadata loaded;
    GraphMetadataParquetLoader::load(commitDir, loaded);

    EXPECT_TRUE(GraphMetadataComparator::same(original, loaded));
}
