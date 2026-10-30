#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "datapart/EdgeRecord.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/CommitStatistics.h"
#include "versioning/Tombstones.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The live edges from a node carrying every label of source to one carrying every label of
// target, of edgeType or of every type, counted one by one
size_t countEdgesOneByOne(const GraphReader& reader,
                          const LabelSet& source,
                          std::optional<EdgeTypeID> edgeType,
                          const LabelSet& target) {
    size_t count = 0;
    for (const EdgeRecord& edge : reader.scanOutEdges()) {
        if (reader.edgeIsDeleted(edge._edgeID)) {
            continue;
        }

        const bool hasTheType = !edgeType || edge._edgeTypeID == *edgeType;
        const bool leavesTheSource = reader.getNodeLabelSet(edge._nodeID).hasAtLeastLabels(source.handle());
        const bool reachesTheTarget = reader.getNodeLabelSet(edge._otherID).hasAtLeastLabels(target.handle());

        if (hasTheType && leavesTheSource && reachesTheTarget) {
            count++;
        }
    }

    return count;
}

EdgeID findEdge(const GraphReader& reader, NodeID source, std::string_view edgeTypeName, NodeID target) {
    const std::optional<EdgeTypeID> edgeType = reader.getMetadata().edgeTypes().get(edgeTypeName);
    for (const EdgeRecord& edge : reader.scanOutEdges()) {
        if (edge._nodeID == source && edge._otherID == target && edge._edgeTypeID == edgeType) {
            return edge._edgeID;
        }
    }

    ADD_FAILURE() << "No " << edgeTypeName << " edge from " << source << " to " << target;
    return EdgeID {};
}

// Every label set the graph holds, and the empty one every node matches
void collectLabelSets(const GraphMetadata& metadata, std::vector<LabelSet>& labelSets) {
    labelSets.emplace_back();
    for (const LabelSetMap::Pair& pair : metadata.labelsets()) {
        labelSets.push_back(*pair._value);
    }
}

void expectCountsMatchEveryEdge(const GraphReader& reader) {
    const GraphMetadata& metadata = reader.getMetadata();

    std::vector<LabelSet> labelSets;
    collectLabelSets(metadata, labelSets);

    for (const LabelSet& source : labelSets) {
        for (const LabelSet& target : labelSets) {
            EXPECT_EQ(reader.getEdgeCount(source.handle(), target.handle()),
                      countEdgesOneByOne(reader, source, std::nullopt, target));

            for (const EdgeTypeMap::Pair& edgeType : metadata.edgeTypes()) {
                EXPECT_EQ(reader.getEdgeCount(source.handle(), edgeType._id, target.handle()),
                          countEdgesOneByOne(reader, source, edgeType._id, target));
            }
        }
    }
}

}

class EdgeCountsTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    size_t countParts() {
        const FrozenCommitTx transaction = _graph->openTransaction();
        return transaction.readGraph().getDatapartCount();
    }

    size_t countKnowsWell(const GraphReader& reader, std::string_view sourceLabel, std::string_view targetLabel) {
        const GraphMetadata& metadata = reader.getMetadata();
        const std::optional<EdgeTypeID> knowsWell = metadata.edgeTypes().get("KNOWS_WELL");
        const std::optional<LabelID> source = metadata.labels().get(sourceLabel);
        const std::optional<LabelID> target = metadata.labels().get(targetLabel);
        if (!knowsWell || !source || !target) {
            ADD_FAILURE() << "simpledb lacks KNOWS_WELL, " << sourceLabel << " or " << targetLabel;
            return 0;
        }

        LabelSet sourceLabels;
        LabelSet targetLabels;
        sourceLabels.set(*source);
        targetLabels.set(*target);

        return reader.getEdgeCount(sourceLabels.handle(), *knowsWell, targetLabels.handle());
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;

    static constexpr NodeID REMY {0};
    static constexpr NodeID ADAM {1};
    static constexpr NodeID GHOSTS {6};
};

TEST_F(EdgeCountsTest, countsTheEdgesOfSimpleDB) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    EXPECT_EQ(countKnowsWell(reader, "Person", "Person"), 2u);
    EXPECT_EQ(countKnowsWell(reader, "Interest", "Person"), 1u);
    EXPECT_EQ(countKnowsWell(reader, "Person", "Interest"), 0u);

    expectCountsMatchEveryEdge(reader);
}

// A later part holds edges between nodes of the earlier one, out of them into its own nodes,
// and out of its own nodes into them
TEST_F(EdgeCountsTest, countsTheEdgesALaterPartAddsToEarlierNodes) {
    const size_t simpleDBParts = countParts();

    GraphWriter writer(_graph.get(), _jobSystem.get());
    const NodeID chess = writer.addNode({"Interest"});
    const NodeID bob = writer.addNode({"Person", "Founder"});
    writer.addEdge("KNOWS_WELL", REMY, ADAM);
    writer.addEdge("INTERESTED_IN", REMY, chess);
    writer.addEdge("KNOWS_WELL", bob, REMY);
    writer.addEdge("KNOWS_WELL", GHOSTS, bob);
    writer.addEdge("LIKES", bob, chess);
    ASSERT_TRUE(writer.submit());

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ASSERT_GT(reader.getDatapartCount(), simpleDBParts);

    EXPECT_EQ(countKnowsWell(reader, "Person", "Person"), 4u);
    EXPECT_EQ(countKnowsWell(reader, "Interest", "Person"), 2u);

    expectCountsMatchEveryEdge(reader);
}

TEST_F(EdgeCountsTest, leavesOutTheEdgesATombstoneDeletes) {
    GraphWriter writer(_graph.get(), _jobSystem.get());
    const NodeID chess = writer.addNode({"Interest"});
    const EdgeRecord remyAdam = writer.addEdge("KNOWS_WELL", REMY, ADAM);
    writer.addEdge("INTERESTED_IN", ADAM, chess);
    ASSERT_TRUE(writer.submit());

    EdgeID adamRemy;
    {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        EXPECT_EQ(countKnowsWell(reader, "Person", "Person"), 3u);

        adamRemy = findEdge(reader, ADAM, "KNOWS_WELL", REMY);
    }

    // One edge of the first part and one of the second
    writer.deleteEdge(adamRemy);
    writer.deleteEdge(remyAdam._edgeID);
    ASSERT_TRUE(writer.submit());

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    ASSERT_TRUE(reader.edgeIsDeleted(adamRemy));

    EXPECT_EQ(countKnowsWell(reader, "Person", "Person"), 1u);
    EXPECT_EQ(countKnowsWell(reader, "Interest", "Person"), 1u);

    expectCountsMatchEveryEdge(reader);
}

// A commit still being built is read before it gains its parts and tombstones, so a sum
// taken on fewer of either is taken again
TEST_F(EdgeCountsTest, sumsAgainOnceTheCommitHoldsMorePartsOrTombstones) {
    const size_t simpleDBParts = countParts();

    GraphWriter writer(_graph.get(), _jobSystem.get());
    writer.addEdge("KNOWS_WELL", REMY, ADAM);
    ASSERT_TRUE(writer.submit());

    {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        writer.deleteEdge(findEdge(reader, ADAM, "KNOWS_WELL", REMY));
    }

    ASSERT_TRUE(writer.submit());

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView view = reader.getView();
    const DataPartSpan parts = view.dataparts();
    ASSERT_GT(parts.size(), simpleDBParts);

    const GraphMetadata& metadata = reader.getMetadata();
    const std::optional<EdgeTypeID> knowsWell = metadata.edgeTypes().get("KNOWS_WELL");
    const std::optional<LabelID> person = metadata.labels().get("Person");
    ASSERT_TRUE(knowsWell && person);

    LabelSet persons;
    persons.set(*person);

    const Tombstones noTombstones;
    CommitStatistics cache;
    EXPECT_EQ(cache.countEdges(parts.first(simpleDBParts), noTombstones, persons.handle(), *knowsWell, persons.handle()), 2u);
    EXPECT_EQ(cache.countEdges(parts, noTombstones, persons.handle(), *knowsWell, persons.handle()), 3u);
    EXPECT_EQ(cache.countEdges(parts, view.tombstones(), persons.handle(), *knowsWell, persons.handle()), 2u);
}

// Two changes made side by side each create a label and an edge type, so the one submitted
// second is rebased and its parts' counts take the IDs main gave its labels and types
TEST_F(EdgeCountsTest, countsTheEdgesOfARebasedChange) {
    GraphWriter robots(_graph.get(), _jobSystem.get());
    const NodeID robot = robots.addNode({"Robot"});
    const NodeID factory = robots.addNode({"Robot", "Factory"});
    robots.addEdge("BUILDS", factory, robot);
    robots.addEdge("BUILDS", factory, REMY);
    ASSERT_TRUE(robots.commit());

    GraphWriter planets(_graph.get(), _jobSystem.get());
    const NodeID mars = planets.addNode({"Planet"});
    const NodeID phobos = planets.addNode({"Planet", "Moon"});
    planets.addEdge("ORBITS", phobos, mars);
    planets.addEdge("ORBITS", ADAM, mars);
    ASSERT_TRUE(planets.submit());

    ASSERT_TRUE(robots.submit());

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphMetadata& metadata = reader.getMetadata();

    const std::optional<EdgeTypeID> builds = metadata.edgeTypes().get("BUILDS");
    const std::optional<EdgeTypeID> orbits = metadata.edgeTypes().get("ORBITS");
    const std::optional<LabelID> robotLabel = metadata.labels().get("Robot");
    const std::optional<LabelID> planetLabel = metadata.labels().get("Planet");
    ASSERT_TRUE(builds && orbits && robotLabel && planetLabel);

    LabelSet robotLabels;
    LabelSet planetLabels;
    const LabelSet anyLabels;
    robotLabels.set(*robotLabel);
    planetLabels.set(*planetLabel);

    EXPECT_EQ(reader.getEdgeCount(robotLabels.handle(), *builds, robotLabels.handle()), 1u);
    EXPECT_EQ(reader.getEdgeCount(robotLabels.handle(), *builds, anyLabels.handle()), 2u);
    EXPECT_EQ(reader.getEdgeCount(anyLabels.handle(), *orbits, planetLabels.handle()), 2u);

    expectCountsMatchEveryEdge(reader);
}
