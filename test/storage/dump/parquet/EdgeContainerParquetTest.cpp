#include "TuringTest.h"

#include "dump/parquet/EdgeContainerParquetDumper.h"
#include "dump/parquet/EdgeContainerParquetLoader.h"
#include "comparators/EdgeContainerComparator.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"

#include <stddef.h>

#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

#include "ID.h"
#include "Path.h"

using namespace db;
using namespace turing::test;

class EdgeContainerParquetTest : public TuringTest {
protected:
    void initialize() override {
    }
};

TEST_F(EdgeContainerParquetTest, RoundTrip) {
    const NodeID firstNodeID = 0;
    const EdgeID firstEdgeID = 0;

    std::vector<EdgeRecord> outEdges = {
        {0, 1, 2, 0},
        {1, 3, 4, 1},
        {2, 4, 3, 0},
        {3, 1, 8, 2},
        {4, 6, 7, 1},
    };

    std::unordered_map<EdgeID, EdgeID> tmpToFinalEdgeIDs;
    const std::unique_ptr<EdgeContainer> original = EdgeContainer::create(firstNodeID,
                                                                          firstEdgeID,
                                                                          std::move(outEdges),
                                                                          tmpToFinalEdgeIDs);
    ASSERT_NE(original, nullptr);

    const fs::Path outPath = fs::Path(_outDir) / "edges-out.parquet";
    const fs::Path inPath = fs::Path(_outDir) / "edges-in.parquet";

    EdgeContainerParquetDumper::dump(*original, outPath, inPath);

    const std::unique_ptr<EdgeContainer> loaded =
        EdgeContainerParquetLoader::load(outPath, inPath);

    EXPECT_TRUE(EdgeContainerComparator::same(*original, *loaded));

    EXPECT_EQ(loaded->getFirstEdgeID().getValue(), firstEdgeID.getValue());
    EXPECT_EQ(loaded->getFirstNodeID().getValue(), firstNodeID.getValue());

    // Dump-everything: the comparator only checks out-edges, so verify the in-edge
    // direction round-trips too.
    const std::span<const EdgeRecord> originalIns = original->getIns();
    const std::span<const EdgeRecord> loadedIns = loaded->getIns();
    ASSERT_EQ(originalIns.size(), loadedIns.size());
    for (size_t i = 0; i < originalIns.size(); ++i) {
        EXPECT_EQ(originalIns[i]._edgeID.getValue(), loadedIns[i]._edgeID.getValue());
        EXPECT_EQ(originalIns[i]._nodeID.getValue(), loadedIns[i]._nodeID.getValue());
        EXPECT_EQ(originalIns[i]._otherID.getValue(), loadedIns[i]._otherID.getValue());
        EXPECT_EQ(originalIns[i]._edgeTypeID.getValue(), loadedIns[i]._edgeTypeID.getValue());
    }
}
