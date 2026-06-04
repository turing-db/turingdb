#include "TuringTest.h"

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "dump/parquet/DataPartParquetDumper.h"
#include "dump/parquet/DataPartParquetLoader.h"
#include "dump/parquet/DataPartParquetLayout.h"
#include "dump/parquet/PropertyIndexerParquetLayout.h"
#include "comparators/DataPartComparator.h"
#include "datapart/DataPart.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSetMap.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "writers/GraphWriter.h"
#include "datapart/EdgeRecord.h"
#include "indexers/EdgeIndexer.h"
#include "indexers/PropertyIndexer.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyManager.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <vector>

#include "JobSystem.h"
#include "Path.h"
#include "FatalException.h"

using namespace db;
using namespace turing::test;

namespace {

// Overwrites a dumped part's node property indexer with a single forged range.
void writeNodePropertyIndexer(const fs::Path& partDir,
                              int64_t propertyTypeId,
                              int64_t labelsetId,
                              int64_t offset,
                              int64_t count) {
    namespace layout = propertyIndexerParquetLayout;

    ParquetWriteSchema schema;
    schema.addColumn(layout::PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::OFFSET_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(layout::COUNT_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(dataPartParquetLayout::nodePropIndexer(partDir), schema);

    const std::vector<int64_t> propertyTypeIds {propertyTypeId};
    const std::vector<int64_t> labelsetIds {labelsetId};
    const std::vector<int64_t> offsets {offset};
    const std::vector<int64_t> counts {count};

    writer.beginRowGroup(1);
    writer.writeInt64Column(0, propertyTypeIds);
    writer.writeInt64Column(1, labelsetIds);
    writer.writeInt64Column(2, offsets);
    writer.writeInt64Column(3, counts);
    writer.finish();
}

}

class DataPartParquetTest : public TuringTest {
public:
    void initialize() override {
        _graphPath = fs::Path {_outDir} / "graph";
        _graph = Graph::create("test", _graphPath);
        SimpleGraph::createSimpleGraph(_graph.get());
    }

protected:
    fs::Path _graphPath;
    std::unique_ptr<Graph> _graph;
};

TEST_F(DataPartParquetTest, RoundTrip) {
    const auto tx = _graph->openTransaction();
    const auto reader = tx.readGraph();
    const auto parts = reader.dataparts();
    const LabelSetMap& labelsets = reader.getMetadata().labelsets();

    ASSERT_FALSE(parts.empty());

    for (size_t i = 0; i < parts.size(); ++i) {
        const DataPart& part = *parts[i];

        const fs::Path partDir = fs::Path {_outDir} / ("part-" + std::to_string(i));
        DataPartParquetDumper::dump(part, partDir);

        const std::unique_ptr<DataPart> loaded = DataPartParquetLoader::load(partDir, labelsets);

        EXPECT_TRUE(DataPartComparator::same(part, *loaded))
            << "DataPart " << i << " did not round-trip";
    }
}

// The indexer file is read before the property containers, so range validation runs
// once the containers are loaded; a forged range past the container's values must be
// refused instead of installing an index that reads out of bounds.
TEST_F(DataPartParquetTest, OutOfBoundsPropertyRangeThrows) {
    const auto tx = _graph->openTransaction();
    const auto reader = tx.readGraph();
    const auto parts = reader.dataparts();
    const LabelSetMap& labelsets = reader.getMetadata().labelsets();

    ASSERT_FALSE(parts.empty());
    const DataPart& part = *parts[0];

    const PropertyIndexer& indexers = part.nodeProperties().indexers();
    ASSERT_FALSE(indexers.empty());

    const PropertyTypeID propertyTypeID = indexers.begin()->first;
    const LabelSetHandle labelset = indexers.begin()->second.begin()->first;

    const fs::Path partDir = fs::Path {_outDir} / "forged-range";
    DataPartParquetDumper::dump(part, partDir);

    writeNodePropertyIndexer(partDir,
                             static_cast<int64_t>(propertyTypeID.getValue()),
                             static_cast<int64_t>(labelset.getID().getValue()),
                             1000000,
                             1);

    EXPECT_THROW(DataPartParquetLoader::load(partDir, labelsets), FatalException);
}

// An indexer entry whose property type has no dumped container must be refused.
TEST_F(DataPartParquetTest, IndexerForMissingContainerThrows) {
    const auto tx = _graph->openTransaction();
    const auto reader = tx.readGraph();
    const auto parts = reader.dataparts();
    const LabelSetMap& labelsets = reader.getMetadata().labelsets();

    ASSERT_FALSE(parts.empty());
    const DataPart& part = *parts[0];

    const PropertyIndexer& indexers = part.nodeProperties().indexers();
    ASSERT_FALSE(indexers.empty());

    const LabelSetHandle labelset = indexers.begin()->second.begin()->first;

    const fs::Path partDir = fs::Path {_outDir} / "forged-property-type";
    DataPartParquetDumper::dump(part, partDir);

    writeNodePropertyIndexer(partDir,
                             999999,
                             static_cast<int64_t>(labelset.getID().getValue()),
                             0,
                             1);

    EXPECT_THROW(DataPartParquetLoader::load(partDir, labelsets), FatalException);
}

// Builds a two-commit graph where the second commit adds an edge into a node from
// the first commit, so the second DataPart records that node as a patch node. This
// exercises the EdgeIndexer patch path (the patch-prefix/core-suffix split and the
// _patchNodeOffsets rebuild on load) with non-empty data.
class DataPartParquetPatchTest : public TuringTest {
public:
    void initialize() override {
        _graphPath = fs::Path {_outDir} / "patch-graph";
        _graph = Graph::create("patch", _graphPath);

        JobSystem jobSystem;
        jobSystem.init();
        GraphWriter writer(_graph.get(), &jobSystem);

        const NodeID a = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(a, "name", "A");
        const NodeID b = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(b, "name", "B");
        writer.addEdge("KNOWS", a, b);
        writer.submit();

        // Second commit: a new node c with an edge into the existing node a, which
        // makes a a patch node in the second DataPart.
        const NodeID c = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(c, "name", "C");
        writer.addEdge("KNOWS", c, a);
        writer.submit();

        _patchedNode = a;
    }

protected:
    fs::Path _graphPath;
    std::unique_ptr<Graph> _graph;
    NodeID _patchedNode {0};
};

TEST_F(DataPartParquetPatchTest, PatchRoundTrip) {
    const auto tx = _graph->openTransaction();
    const auto reader = tx.readGraph();
    const auto parts = reader.dataparts();
    const LabelSetMap& labelsets = reader.getMetadata().labelsets();

    bool foundPatch = false;
    for (size_t i = 0; i < parts.size(); ++i) {
        const DataPart& part = *parts[i];
        const size_t patchNodeCount = part.edgeIndexer().getPatchNodeCount();

        const fs::Path partDir = fs::Path {_outDir} / ("part-" + std::to_string(i));
        DataPartParquetDumper::dump(part, partDir);
        const std::unique_ptr<DataPart> loaded = DataPartParquetLoader::load(partDir, labelsets);

        EXPECT_TRUE(DataPartComparator::same(part, *loaded));
        EXPECT_EQ(loaded->edgeIndexer().getPatchNodeCount(), patchNodeCount);

        if (patchNodeCount > 0) {
            foundPatch = true;

            // The comparator only checks the label-set spans, so verify the patched
            // node's in-edges directly — this read resolves through _patchNodeOffsets.
            const std::span<const EdgeRecord> originalInEdges =
                part.edgeIndexer().getNodeInEdges(_patchedNode);
            const std::span<const EdgeRecord> loadedInEdges =
                loaded->edgeIndexer().getNodeInEdges(_patchedNode);

            ASSERT_EQ(originalInEdges.size(), loadedInEdges.size());
            EXPECT_GT(originalInEdges.size(), 0u);
            for (size_t e = 0; e < originalInEdges.size(); ++e) {
                EXPECT_EQ(originalInEdges[e]._edgeID.getValue(), loadedInEdges[e]._edgeID.getValue());
                EXPECT_EQ(originalInEdges[e]._nodeID.getValue(), loadedInEdges[e]._nodeID.getValue());
                EXPECT_EQ(originalInEdges[e]._otherID.getValue(), loadedInEdges[e]._otherID.getValue());
            }
        }
    }

    ASSERT_TRUE(foundPatch) << "expected at least one DataPart with patch nodes";
}
