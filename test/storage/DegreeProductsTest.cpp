#include <gtest/gtest.h>

#include <stddef.h>

#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "datapart/DegreeProducts.h"
#include "datapart/EdgeRecord.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/GraphWriter.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using DegreeKey = std::pair<EdgeTypeID::Type, bool>;
using NodeDegrees = std::map<NodeID, std::map<DegreeKey, size_t>>;

// Each node's degree in the edges of every type and side, counted edge by edge
void countDegrees(const GraphReader& reader, NodeDegrees& degrees) {
    for (const EdgeRecord& edge : reader.scanOutEdges()) {
        degrees[edge._nodeID][{edge._edgeTypeID.getValue(), true}]++;
        degrees[edge._otherID][{edge._edgeTypeID.getValue(), false}]++;
    }
}

bool selects(const DegreeSelection& selection, const DegreeKey& degree) {
    if (selection._isOut != degree.second) {
        return false;
    }

    if (selection._isEveryType) {
        return true;
    }

    for (const EdgeTypeID edgeType : selection._edgeTypes) {
        if (edgeType.getValue() == degree.first) {
            return true;
        }
    }

    return false;
}

// The pairs of two distinct edges, one each side selects, at the nodes carrying labels
size_t sumProductsOneByOne(const GraphReader& reader,
                           const NodeDegrees& degrees,
                           const LabelSet& labels,
                           const DegreeSelection& first,
                           const DegreeSelection& second) {
    size_t sum = 0;
    for (const auto& [nodeID, nodeDegrees] : degrees) {
        if (!reader.getNodeLabelSet(nodeID).hasAtLeastLabels(labels.handle())) {
            continue;
        }

        size_t firstDegree = 0;
        size_t secondDegree = 0;
        size_t sharedDegree = 0;
        for (const auto& [degree, count] : nodeDegrees) {
            const bool isFirst = selects(first, degree);
            const bool isSecond = selects(second, degree);

            firstDegree += isFirst ? count : 0;
            secondDegree += isSecond ? count : 0;
            sharedDegree += isFirst && isSecond ? count : 0;
        }

        sum += firstDegree * secondDegree - sharedDegree;
    }

    return sum;
}

void expectProductsMatchEveryNode(const GraphReader& reader) {
    const GraphMetadata& metadata = reader.getMetadata();

    NodeDegrees degrees;
    countDegrees(reader, degrees);

    std::vector<LabelSet> labelSets;
    labelSets.emplace_back();
    for (const LabelSetMap::Pair& pair : metadata.labelsets()) {
        labelSets.push_back(*pair._value);
    }

    std::vector<EdgeTypeID> edgeTypes;
    for (const EdgeTypeMap::Pair& edgeType : metadata.edgeTypes()) {
        edgeTypes.push_back(edgeType._id);
    }

    std::vector<DegreeSelection> selections;
    for (const bool isOut : {true, false}) {
        selections.push_back(DegreeSelection {._isEveryType = true, ._isOut = isOut});
        for (const EdgeTypeID& edgeType : edgeTypes) {
            selections.push_back(DegreeSelection {._edgeTypes = std::span(&edgeType, 1), ._isOut = isOut});
        }
    }

    for (const LabelSet& labels : labelSets) {
        for (const DegreeSelection& first : selections) {
            for (const DegreeSelection& second : selections) {
                EXPECT_EQ(reader.sumDegreeProducts(labels.handle(), first, second),
                          sumProductsOneByOne(reader, degrees, labels, first, second));
            }
        }
    }
}

}

class DegreeProductsTest : public TuringTest {
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

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;

    static constexpr NodeID REMY {0};
    static constexpr NodeID ADAM {1};
    static constexpr NodeID GHOSTS {6};
};

TEST_F(DegreeProductsTest, sumsTheProductsOfSimpleDB) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const GraphMetadata& metadata = reader.getMetadata();
    const std::optional<EdgeTypeID> knowsWell = metadata.edgeTypes().get("KNOWS_WELL");
    const std::optional<LabelID> person = metadata.labels().get("Person");
    ASSERT_TRUE(knowsWell && person);

    LabelSet persons;
    persons.set(*person);

    // Remy and Adam know each other well, and Ghosts knows Remy well: Remy has 2 KNOWS_WELL
    // in-edges and 1 out-edge, Adam 1 of each
    const DegreeSelection knownBy {._edgeTypes = std::span(&*knowsWell, 1), ._isOut = false};
    const DegreeSelection knows {._edgeTypes = std::span(&*knowsWell, 1), ._isOut = true};
    EXPECT_EQ(reader.sumDegreeProducts(persons.handle(), knownBy, knows), 3u);
    EXPECT_EQ(reader.sumDegreeProducts(persons.handle(), knownBy, knownBy), 2u);

    expectProductsMatchEveryNode(reader);
}

// A later part adds edges to nodes of earlier ones, whose products then count their earlier
// edges with the ones it adds
TEST_F(DegreeProductsTest, sumsTheEdgesALaterPartAddsToEarlierNodes) {
    GraphWriter writer(_graph.get(), _jobSystem.get());
    const NodeID chess = writer.addNode({"Interest"});
    const NodeID bob = writer.addNode({"Person", "Founder"});
    writer.addEdge("KNOWS_WELL", REMY, ADAM);
    writer.addEdge("KNOWS_WELL", REMY, bob);
    writer.addEdge("INTERESTED_IN", REMY, chess);
    writer.addEdge("KNOWS_WELL", bob, REMY);
    writer.addEdge("KNOWS_WELL", GHOSTS, bob);
    ASSERT_TRUE(writer.submit());

    writer.addEdge("KNOWS_WELL", ADAM, bob);
    writer.addEdge("INTERESTED_IN", bob, chess);
    ASSERT_TRUE(writer.submit());

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    expectProductsMatchEveryNode(reader);
}

// Two changes made side by side add edges to the same node, so the one submitted second is
// rebased onto edges its parts were not built against
TEST_F(DegreeProductsTest, sumsTheProductsOfARebasedChange) {
    GraphWriter robots(_graph.get(), _jobSystem.get());
    const NodeID robot = robots.addNode({"Robot"});
    robots.addEdge("BUILDS", robot, REMY);
    robots.addEdge("KNOWS_WELL", REMY, robot);
    ASSERT_TRUE(robots.commit());

    GraphWriter planets(_graph.get(), _jobSystem.get());
    const NodeID mars = planets.addNode({"Planet"});
    planets.addEdge("ORBITS", REMY, mars);
    planets.addEdge("KNOWS_WELL", REMY, mars);
    ASSERT_TRUE(planets.submit());

    ASSERT_TRUE(robots.submit());

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    expectProductsMatchEveryNode(reader);
}
