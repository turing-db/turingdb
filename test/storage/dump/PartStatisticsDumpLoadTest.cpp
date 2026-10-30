#include "TuringTest.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "dump/GraphDumper.h"
#include "dump/GraphLoader.h"
#include "Graph.h"
#include "JobSystem.h"
#include "Path.h"
#include "SimpleGraph.h"
#include "TuringException.h"
#include "comparators/DegreeProductsComparator.h"
#include "comparators/EdgeCountsComparator.h"
#include "comparators/GraphComparator.h"
#include "datapart/DataPart.h"
#include "datapart/EdgeRecord.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

using namespace db;
using namespace turing::test;

class PartStatisticsDumpLoadTest : public TuringTest {
public:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _dumpPath = fs::Path {_outDir} / "dump";
        _graph = Graph::create("simple", fs::Path {_outDir} / "simple");
        SimpleGraph::createSimpleGraph(_graph.get());

        // Edges of a later part reaching earlier nodes, and one deleted
        GraphWriter writer(_graph.get(), _jobSystem.get());
        const NodeID remy {0};
        const NodeID adam {1};
        const NodeID chess = writer.addNode({"Interest"});
        const EdgeRecord remyAdam = writer.addEdge("KNOWS_WELL", remy, adam);
        writer.addEdge("INTERESTED_IN", adam, chess);
        writer.submit();

        writer.deleteEdge(remyAdam._edgeID);
        writer.submit();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

protected:
    std::unique_ptr<JobSystem> _jobSystem;
    fs::Path _dumpPath;
    std::unique_ptr<Graph> _graph;

    void dumpGraph() {
        auto res = GraphDumper::dump(_graph.get(), _dumpPath);
        if (!res) {
            throw TuringException("Failed to dump graph:\n" + res.error().fmtMessage());
        }
    }

    void expectLoadedStatisticsMatch() {
        auto loadedGraph = Graph::create();
        const auto loadRes = GraphLoader::load(loadedGraph.get(), _dumpPath);
        ASSERT_TRUE(loadRes) << loadRes.error().fmtMessage();

        const FrozenCommitTx originalTx = _graph->openTransaction();
        const FrozenCommitTx loadedTx = loadedGraph->openTransaction();
        const GraphReader original = originalTx.readGraph();
        const GraphReader loaded = loadedTx.readGraph();

        const DataPartSpan originalParts = original.dataparts();
        const DataPartSpan loadedParts = loaded.dataparts();
        ASSERT_EQ(originalParts.size(), loadedParts.size());

        for (size_t i = 0; i < originalParts.size(); i++) {
            EXPECT_TRUE(EdgeCountsComparator::same(originalParts[i]->edgeCounts(), loadedParts[i]->edgeCounts()))
                << "Mismatching edge counts in part " << i;
            EXPECT_TRUE(DegreeProductsComparator::same(originalParts[i]->degreeProducts(), loadedParts[i]->degreeProducts()))
                << "Mismatching degree products in part " << i;
        }

        const GraphMetadata& metadata = loaded.getMetadata();
        const std::optional<EdgeTypeID> knowsWell = metadata.edgeTypes().get("KNOWS_WELL");
        const std::optional<LabelID> person = metadata.labels().get("Person");
        ASSERT_TRUE(knowsWell && person);

        LabelSet persons;
        persons.set(*person);

        EXPECT_EQ(loaded.getEdgeCount(persons.handle(), *knowsWell, persons.handle()), 2u);
    }
};

TEST_F(PartStatisticsDumpLoadTest, loadsTheStatisticsItDumped) {
    dumpGraph();
    expectLoadedStatisticsMatch();
}

// A part dumped before parts kept statistics has none of their files, and its degree
// products are taken against the parts loaded before it
TEST_F(PartStatisticsDumpLoadTest, computesTheStatisticsOfADumpWithoutThem) {
    dumpGraph();

    const auto dataparts = (_dumpPath / "dataparts").listDir();
    ASSERT_TRUE(dataparts);
    ASSERT_FALSE(dataparts->empty());

    for (const fs::Path& partDir : *dataparts) {
        for (const std::string_view fileName : {"edge-counts", "degree-products"}) {
            const fs::Path statisticsFile = partDir / std::string(fileName);
            ASSERT_TRUE(statisticsFile.exists()) << statisticsFile.get();
            ASSERT_TRUE(statisticsFile.rm());
        }
    }

    expectLoadedStatisticsMatch();
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv);
}
