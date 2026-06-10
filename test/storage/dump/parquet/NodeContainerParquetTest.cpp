#include "TuringTest.h"

#include "dump/parquet/NodeContainerParquetDumper.h"
#include "dump/parquet/NodeContainerParquetLoader.h"
#include "comparators/NodeContainerComparator.h"
#include "datapart/NodeContainer.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/LabelSetMap.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class NodeContainerParquetTest : public TuringTest {
protected:
    void initialize() override {
    }
};

TEST_F(NodeContainerParquetTest, RoundTrip) {
    LabelSetMap labelsets;
    const LabelSet labelSetA = LabelSet::fromList({LabelID {0}});
    const LabelSet labelSetB = LabelSet::fromList({LabelID {1}, LabelID {3}});
    const LabelSet labelSetC = LabelSet::fromList({LabelID {2}});

    const LabelSetHandle handleA = labelsets.getOrCreate(labelSetA);
    const LabelSetHandle handleB = labelsets.getOrCreate(labelSetB);
    const LabelSetHandle handleC = labelsets.getOrCreate(labelSetC);

    // Per-node label sets laid out as a real datapart does: grouped into one
    // contiguous run per label set, ascending by labelset id (NodeContainer::create
    // requires this and builds one range per label set).
    const std::vector<LabelSetHandle> perNode {
        handleA, handleA, handleA,
        handleB, handleB,
        handleC, handleC};

    const NodeID firstID {1000};
    const std::unique_ptr<NodeContainer> original = NodeContainer::create(firstID, perNode);
    ASSERT_NE(original, nullptr);

    const fs::Path rangesPath = fs::Path(_outDir) / "node-ranges.parquet";
    const fs::Path recordsPath = fs::Path(_outDir) / "node-records.parquet";

    NodeContainerParquetDumper::dump(*original, rangesPath, recordsPath);

    const std::unique_ptr<NodeContainer> loaded =
        NodeContainerParquetLoader::load(rangesPath, recordsPath, labelsets);

    EXPECT_TRUE(NodeContainerComparator::same(*original, *loaded));

    EXPECT_EQ(loaded->getFirstNodeID().getValue(), firstID.getValue());

    // Dump-everything: the comparator only checks ranges, so verify the per-node
    // records round-trip too.
    ASSERT_EQ(original->records().size(), loaded->records().size());
    for (size_t i = 0; i < original->records().size(); ++i) {
        EXPECT_EQ(original->records()[i]._labelset.getID().getValue(),
                  loaded->records()[i]._labelset.getID().getValue());
    }
}
