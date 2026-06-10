#include "TuringTest.h"

#include "dump/parquet/PropertyIndexerParquetDumper.h"
#include "dump/parquet/PropertyIndexerParquetLoader.h"
#include "comparators/PropertyIndexerComparator.h"
#include "indexers/PropertyIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/LabelSetMap.h"

#include <stddef.h>

#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class PropertyIndexerParquetTest : public TuringTest {
protected:
    void initialize() override {
    }
};

TEST_F(PropertyIndexerParquetTest, RoundTrip) {
    LabelSetMap labelsets;
    const LabelSet labelSetA = LabelSet::fromList({LabelID {0}});
    const LabelSet labelSetB = LabelSet::fromList({LabelID {1}});
    const LabelSetHandle handleA = labelsets.getOrCreate(labelSetA);
    const LabelSetHandle handleB = labelsets.getOrCreate(labelSetB);

    const PropertyTypeID propertyTypeA {0};
    const PropertyTypeID propertyTypeB {1};

    PropertyIndexer original;
    // (propertyTypeA, labelSetA) has two ranges — exercises range ordering.
    original[propertyTypeA][handleA].push_back(PropertyRange {0, 3});
    original[propertyTypeA][handleA].push_back(PropertyRange {10, 2});
    original[propertyTypeA][handleB].push_back(PropertyRange {3, 5});
    original[propertyTypeB][handleA].push_back(PropertyRange {0, 7});

    const fs::Path path = fs::Path(_outDir) / "node-prop-indexer.parquet";
    PropertyIndexerParquetDumper::dump(original, path);

    PropertyIndexer loaded;
    PropertyIndexerParquetLoader::load(path, labelsets, loaded);

    EXPECT_TRUE(PropertyIndexerComparator::same(original, loaded));
    EXPECT_EQ(loaded.size(), original.size());

    // The comparator zips ranges without checking lengths, so verify the multi-range
    // group explicitly (order matters: it is a vector).
    const auto propertyTypeAIt = loaded.find(propertyTypeA);
    ASSERT_NE(propertyTypeAIt, loaded.end());
    const LabelSetPropertyIndexer& labelSetIndexer = propertyTypeAIt->second;

    const auto rangesIt = labelSetIndexer.find(handleA);
    ASSERT_NE(rangesIt, labelSetIndexer.end());
    const std::vector<PropertyRange>& ranges = rangesIt->second;

    ASSERT_EQ(ranges.size(), 2u);
    EXPECT_EQ(ranges[0]._offset, 0u);
    EXPECT_EQ(ranges[0]._count, 3u);
    EXPECT_EQ(ranges[1]._offset, 10u);
    EXPECT_EQ(ranges[1]._count, 2u);
}
