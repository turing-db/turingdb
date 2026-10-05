#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string_view>
#include <tuple>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SimpleGraph.h"
#include "metadata/EdgeTypeMap.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelMap.h"
#include "metadata/LabelSetMap.h"
#include "metadata/SchemaGraph.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"

using namespace db;
using namespace turing::test;

// simpledb summarised: 18 edges, none a self-loop, KNOWS_WELL between people and from one
// interest to a person, INTERESTED_IN from people to interests
class SchemaGraphTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    void writeSelfLoop(NodeID node, std::string_view edgeType) {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        const EdgeTypeID type = builder.getMetadata().getOrCreateEdgeType(edgeType);
        builder.addEdge(type, node, node);

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    static LabelSet labels(const GraphView& view, std::initializer_list<std::string_view> names) {
        LabelSet labelSet;
        for (const std::string_view name : names) {
            labelSet.set(view.metadata().labels().get(name).value());
        }

        return labelSet;
    }

    static std::vector<EdgeTypeID> types(const GraphView& view, std::initializer_list<std::string_view> names) {
        std::vector<EdgeTypeID> typeIDs;
        for (const std::string_view name : names) {
            typeIDs.push_back(view.metadata().edgeTypes().get(name).value());
        }

        return typeIDs;
    }

    static SchemaPattern::Edge edge(size_t source, size_t target, std::vector<EdgeTypeID> typeIDs, bool undirected = false) {
        SchemaPattern::Edge patternEdge;
        patternEdge._source = source;
        patternEdge._target = target;
        patternEdge._types = typeIDs;
        patternEdge._undirected = undirected;

        return patternEdge;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
};

TEST_F(SchemaGraphTest, countsEveryEdgeUnderItsEndsAndType) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const SchemaGraph& schema = view.schemaGraph();

    size_t edges = 0;
    size_t selfLoops = 0;
    for (const SchemaArc& arc : schema.arcs()) {
        edges += arc._count;
        selfLoops += arc._selfLoopCount;
    }
    EXPECT_EQ(edges, 18u);
    EXPECT_EQ(selfLoops, 0u);

    const bool sorted = std::ranges::is_sorted(schema.arcs(), [](const SchemaArc& left, const SchemaArc& right) {
        return std::tie(left._source, left._type, left._target) < std::tie(right._source, right._type, right._target);
    });
    EXPECT_TRUE(sorted);

    const GraphReader reader = view.read();
    const LabelSetID remy = reader.getNodeLabelSet(NodeID {0}).getID();
    const LabelSetID adam = reader.getNodeLabelSet(NodeID {1}).getID();
    const EdgeTypeID knowsWell = types(view, {"KNOWS_WELL"}).front();

    const auto remyKnowsAdam = std::ranges::find_if(schema.arcs(), [&](const SchemaArc& arc) {
        return arc._source == remy && arc._type == knowsWell && arc._target == adam;
    });
    ASSERT_NE(remyKnowsAdam, schema.arcs().end());
    EXPECT_EQ(remyKnowsAdam->_count, 1u);
}

TEST_F(SchemaGraphTest, embedsAHopThePartsHold) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    SchemaPattern pattern;
    pattern._nodes = {{labels(view, {"Person"})}, {labels(view, {"Interest"})}};
    pattern._edges = {edge(0, 1, types(view, {"INTERESTED_IN"}))};
    EXPECT_TRUE(view.schemaGraph().embeds(pattern, view.metadata().labelsets()));

    pattern._edges = {edge(0, 1, {})};
    EXPECT_TRUE(view.schemaGraph().embeds(pattern, view.metadata().labelsets()));
}

TEST_F(SchemaGraphTest, embedsNoSelfLoopWhereNoneIs) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    SchemaPattern pattern;
    pattern._nodes = {{labels(view, {"Person"})}};
    pattern._edges = {edge(0, 0, types(view, {"KNOWS_WELL"}))};
    EXPECT_FALSE(view.schemaGraph().embeds(pattern, view.metadata().labelsets()));

    pattern._nodes = {{LabelSet {}}};
    pattern._edges = {edge(0, 0, {})};
    EXPECT_FALSE(view.schemaGraph().embeds(pattern, view.metadata().labelsets()));
}

// Remy and Adam know each other, and no interest is interested in a person
TEST_F(SchemaGraphTest, embedsATwoCycleOnlyWhereTheArcsClose) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    SchemaPattern people;
    people._nodes = {{labels(view, {"Person"})}, {labels(view, {"Person"})}};
    people._edges = {edge(0, 1, types(view, {"KNOWS_WELL"})), edge(1, 0, types(view, {"KNOWS_WELL"}))};
    EXPECT_TRUE(view.schemaGraph().embeds(people, view.metadata().labelsets()));

    SchemaPattern interest;
    interest._nodes = {{labels(view, {"Person"})}, {labels(view, {"Interest"})}};
    interest._edges = {edge(0, 1, types(view, {"INTERESTED_IN"})), edge(1, 0, types(view, {"INTERESTED_IN"}))};
    EXPECT_FALSE(view.schemaGraph().embeds(interest, view.metadata().labelsets()));
}

// Ghosts knows Remy well: an interest to a person, which the undirected edge reads
TEST_F(SchemaGraphTest, readsAnUndirectedEdgeEitherWay) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    SchemaPattern pattern;
    pattern._nodes = {{labels(view, {"Person"})}, {labels(view, {"Interest"})}};
    pattern._edges = {edge(0, 1, types(view, {"KNOWS_WELL"}))};
    EXPECT_FALSE(view.schemaGraph().embeds(pattern, view.metadata().labelsets()));

    pattern._edges = {edge(0, 1, types(view, {"KNOWS_WELL"}), true)};
    EXPECT_TRUE(view.schemaGraph().embeds(pattern, view.metadata().labelsets()));
}

TEST_F(SchemaGraphTest, embedsNoNodeNoLabelSetAllows) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    SchemaPattern pattern;
    pattern._nodes = {{labels(view, {"Person", "Interest"})}};
    EXPECT_FALSE(view.schemaGraph().embeds(pattern, view.metadata().labelsets()));
}

// 40 parts after simpledb's, each adding one node, Even or Odd by turns, and three edges:
// to Remy in the first part, from Remy as a patch edge, and to the node of the part before
TEST_F(SchemaGraphTest, readsLabelSetsAcrossManyParts) {
    constexpr size_t PART_COUNT = 40;
    const size_t simpleDBPartCount = _graph->openTransaction().viewGraph().dataparts().size();

    NodeID previous;
    for (size_t index = 0; index < PART_COUNT; index++) {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();
        MetadataBuilder& metadata = builder.getMetadata();

        LabelSet labelSet;
        labelSet.set(metadata.getOrCreateLabel(index % 2 == 0 ? "Even" : "Odd"));
        const NodeID added = builder.addNode(labelSet);

        builder.addEdge(metadata.getOrCreateEdgeType("TO_REMY"), added, NodeID {0});
        builder.addEdge(metadata.getOrCreateEdgeType("FROM_REMY"), NodeID {0}, added);
        if (index > 0) {
            builder.addEdge(metadata.getOrCreateEdgeType("TO_PREVIOUS"), added, previous);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
        previous = added;
    }

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    ASSERT_EQ(view.dataparts().size(), simpleDBPartCount + PART_COUNT);

    const LabelSetMap& labelSets = view.metadata().labelsets();
    const LabelSetID remy = view.read().getNodeLabelSet(NodeID {0}).getID();
    const LabelSetID even = labelSets.get(labels(view, {"Even"})).value();
    const LabelSetID odd = labelSets.get(labels(view, {"Odd"})).value();

    const auto countOf = [&view](LabelSetID source, std::string_view type, LabelSetID target) {
        const EdgeTypeID typeID = types(view, {type}).front();
        size_t count = 0;
        for (const SchemaArc& arc : view.schemaGraph().arcs()) {
            if (arc._source == source && arc._type == typeID && arc._target == target) {
                count += arc._count;
            }
        }

        return count;
    };

    EXPECT_EQ(countOf(even, "TO_REMY", remy), 20u);
    EXPECT_EQ(countOf(odd, "TO_REMY", remy), 20u);
    EXPECT_EQ(countOf(remy, "FROM_REMY", even), 20u);
    EXPECT_EQ(countOf(remy, "FROM_REMY", odd), 20u);
    EXPECT_EQ(countOf(odd, "TO_PREVIOUS", even), 20u);
    EXPECT_EQ(countOf(even, "TO_PREVIOUS", odd), 19u);
}

// A commit adding a self-loop puts it in the summary of the views that hold the new part,
// and a summary refreshed to such a view picks it up
TEST_F(SchemaGraphTest, refreshesToThePartsAViewHolds) {
    const FrozenCommitTx before = _graph->openTransaction();
    const GraphView viewBefore = before.viewGraph();

    SchemaGraph schema;
    schema.refresh(viewBefore.dataparts(), viewBefore.metadata());
    const size_t arcsBefore = schema.arcs().size();

    writeSelfLoop(NodeID {0}, "KNOWS_WELL");

    const FrozenCommitTx after = _graph->openTransaction();
    const GraphView viewAfter = after.viewGraph();
    ASSERT_GT(viewAfter.dataparts().size(), viewBefore.dataparts().size());

    const auto selfLoops = [](const SchemaGraph& graph) {
        size_t count = 0;
        for (const SchemaArc& arc : graph.arcs()) {
            count += arc._selfLoopCount;
        }

        return count;
    };

    EXPECT_EQ(selfLoops(schema), 0u);
    schema.refresh(viewAfter.dataparts(), viewAfter.metadata());
    EXPECT_EQ(selfLoops(schema), 1u);
    EXPECT_GE(schema.arcs().size(), arcsBefore);

    EXPECT_EQ(selfLoops(viewAfter.schemaGraph()), 1u);

    SchemaPattern loop;
    loop._nodes = {{labels(viewAfter, {"Person"})}};
    loop._edges = {edge(0, 0, types(viewAfter, {"KNOWS_WELL"}))};
    EXPECT_TRUE(viewAfter.schemaGraph().embeds(loop, viewAfter.metadata().labelsets()));
    EXPECT_FALSE(viewBefore.schemaGraph().embeds(loop, viewBefore.metadata().labelsets()));
}
