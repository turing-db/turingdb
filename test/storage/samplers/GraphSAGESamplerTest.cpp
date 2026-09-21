#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "datapart/EdgeRecord.h"
#include "iterators/ChunkConfig.h"
#include "reader/GraphReader.h"
#include "samplers/GraphSAGESampler.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "SimpleGraph.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using NodeCol = GraphSAGESampler::NodeCol;

constexpr size_t hops = GraphSAGESampler::hops;
constexpr size_t columnsPerHop = 3;

static_assert(hops == 3, "The expectations below exepect three hops");

struct SampledEdge {
    uint64_t _src {0};
    uint64_t _tgt {0};

    bool operator==(const SampledEdge& other) const = default;
    auto operator<=>(const SampledEdge& other) const = default;
};

struct HopRows {
    std::vector<SampledEdge> _edges;
    std::vector<uint64_t> _dstNodes;
};

using RunRows = std::array<HopRows, hops>;

struct RunStats {
    size_t _steps {0};
    size_t _widestStep {0};
};

}

class GraphSAGESamplerTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        buildIncidence();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void buildIncidence() {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        ColumnNodeIDs allNodes;
        for (auto it = reader.scanNodes().begin(); it.isValid(); it.next()) {
            const NodeID node = it.get();

            allNodes.push_back(node);
            _nodes.insert(node.getValue());
        }

        for (const EdgeRecord& edge : reader.getOutEdges(&allNodes)) {
            const uint64_t source = edge._nodeID.getValue();
            const uint64_t target = edge._otherID.getValue();

            _incidence[SampledEdge {source, target}]++;
            _incidence[SampledEdge {target, source}]++;
        }
    }

    bool isNode(uint64_t node) const { return _nodes.contains(node); }

    size_t incidenceCount(const SampledEdge& edge) const {
        const auto it = _incidence.find(edge);

        return it == _incidence.cend() ? 0 : it->second;
    }

    bool isIncidentEdge(const SampledEdge& edge) const { return incidenceCount(edge) > 0; }

    void runSample(const ColumnNodeIDs& seeds,
                   const GraphSAGESampler::Fanouts& fanouts,
                   size_t maxRows,
                   size_t rngSeed,
                   RunRows& rows,
                   RunStats& stats) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        std::array<NodeCol, hops> srcs {};
        std::array<NodeCol, hops> tgts {};
        std::array<NodeCol, hops> dstNodes {};

        GraphSAGESampler sampler(reader.getView(), rngSeed);
        for (size_t hop = 0; hop < hops; hop++) {
            sampler.setHopData(hop, &srcs[hop], &tgts[hop], &dstNodes[hop], fanouts[hop]);
        }

        sampler.seed(&seeds);

        rows = RunRows {};
        stats = RunStats {};

        constexpr size_t stepCap = 4096;

        while (!sampler.finished()) {
            sampler.sample(maxRows);
            stats._steps++;

            ASSERT_LE(stats._steps, stepCap) << "sampler never finished";

            const size_t stepRows = dstNodes[0].size();
            stats._widestStep = std::max(stats._widestStep, stepRows);

            ASSERT_LE(stepRows, maxRows) << "step " << stats._steps << " exceeded its budget";

            for (size_t hop = 0; hop < hops; hop++) {
                ASSERT_EQ(dstNodes[hop].size(), stepRows) << "hop " << hop << " dstNodes ragged";
                ASSERT_EQ(srcs[hop].size(), stepRows) << "hop " << hop << " srcs ragged";
                ASSERT_EQ(tgts[hop].size(), stepRows) << "hop " << hop << " tgts ragged";

                collectHop(srcs[hop], tgts[hop], dstNodes[hop], rows[hop]);
            }
        }
    }

    static void collectHop(const NodeCol& srcs,
                           const NodeCol& tgts,
                           const NodeCol& dstNodes,
                           HopRows& out) {
        for (size_t row = 0; row < srcs.size(); row++) {
            const std::optional<NodeID>& src = srcs[row];
            const std::optional<NodeID>& tgt = tgts[row];

            ASSERT_EQ(src.has_value(), tgt.has_value()) << "half a padded edge row";

            if (!src.has_value()) {
                continue;
            }

            out._edges.push_back(SampledEdge {src->getValue(), tgt->getValue()});
        }

        for (const std::optional<NodeID>& node : dstNodes) {
            if (!node.has_value()) {
                continue;
            }

            out._dstNodes.push_back(node->getValue());
        }
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<JobSystem> _jobSystem;

    std::set<uint64_t> _nodes;
    std::map<SampledEdge, size_t> _incidence;
};

TEST_F(GraphSAGESamplerTest, everyEmittedPairIsAnIncidentEdge) {
    const ColumnNodeIDs seeds = {0, 1, 8, 9, 11, 15};
    const GraphSAGESampler::Fanouts fanouts {3, 3, 3};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 11, rows, stats);

    for (size_t hop = 0; hop < hops; hop++) {
        for (const SampledEdge& edge : rows[hop]._edges) {
            EXPECT_TRUE(isIncidentEdge(edge))
                << "hop " << hop << " emitted " << edge._src << " -> " << edge._tgt
                << ", which is not an edge of the graph";
        }
    }
}

TEST_F(GraphSAGESamplerTest, everyDstNodeExists) {
    const ColumnNodeIDs seeds = {0, 8, 12};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 5, rows, stats);

    for (size_t hop = 0; hop < hops; hop++) {
        for (const uint64_t node : rows[hop]._dstNodes) {
            EXPECT_TRUE(isNode(node)) << "hop " << hop << " dst_nodes holds " << node;
        }
    }
}

TEST_F(GraphSAGESamplerTest, firstHopDstNodesAreTheSeeds) {
    const ColumnNodeIDs seeds = {9, 0, 8};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 2, rows, stats);

    std::vector<uint64_t> emitted = rows[0]._dstNodes;
    std::ranges::sort(emitted);

    const std::vector<uint64_t> expected {0, 8, 9};
    EXPECT_EQ(emitted, expected);
}

TEST_F(GraphSAGESamplerTest, repeatedSeedsAreDeduplicated) {
    const ColumnNodeIDs seeds = {0, 0, 0, 8};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 13, rows, stats);

    std::vector<uint64_t> emitted = rows[0]._dstNodes;
    std::ranges::sort(emitted);

    const std::vector<uint64_t> expected {0, 8};
    EXPECT_EQ(emitted, expected);
}

TEST_F(GraphSAGESamplerTest, dstNodesHoldNoDuplicatesWithinAHop) {
    const ColumnNodeIDs seeds = {0, 1, 8, 9, 11};
    const GraphSAGESampler::Fanouts fanouts {3, 3, 3};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 17, rows, stats);

    for (size_t hop = 0; hop < hops; hop++) {
        std::vector<uint64_t> emitted = rows[hop]._dstNodes;
        std::ranges::sort(emitted);

        const auto duplicate = std::ranges::adjacent_find(emitted);
        EXPECT_EQ(duplicate, emitted.end()) << "hop " << hop << " expanded a node twice";
    }
}

TEST_F(GraphSAGESamplerTest, laterHopFrontiersComeFromTheHopBefore) {
    const ColumnNodeIDs seeds = {0, 8, 9};
    const GraphSAGESampler::Fanouts fanouts {3, 3, 3};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 23, rows, stats);

    for (size_t hop = 1; hop < hops; hop++) {
        std::set<uint64_t> reached;
        for (const SampledEdge& edge : rows[hop - 1]._edges) {
            reached.insert(edge._tgt);
        }

        for (const uint64_t node : rows[hop]._dstNodes) {
            EXPECT_TRUE(reached.contains(node))
                << "hop " << hop << " expands " << node << ", which hop " << hop - 1
                << " never reached";
        }
    }
}

TEST_F(GraphSAGESamplerTest, everySourceIsAFrontierNodeOfItsHop) {
    const ColumnNodeIDs seeds = {0, 1, 15, 17};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 29, rows, stats);

    for (size_t hop = 0; hop < hops; hop++) {
        const std::set<uint64_t> frontier(rows[hop]._dstNodes.cbegin(),
                                          rows[hop]._dstNodes.cend());

        for (const SampledEdge& edge : rows[hop]._edges) {
            EXPECT_TRUE(frontier.contains(edge._src))
                << "hop " << hop << " sampled " << edge._src
                << ", which is not in its frontier";
        }
    }
}

TEST_F(GraphSAGESamplerTest, noNodeContributesMoreRowsThanTheFanout) {
    const ColumnNodeIDs seeds = {0, 1, 8, 9, 11, 12, 15, 17};
    const GraphSAGESampler::Fanouts fanouts {2, 3, 1};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 31, rows, stats);

    for (size_t hop = 0; hop < hops; hop++) {
        std::map<uint64_t, size_t> perSource;
        for (const SampledEdge& edge : rows[hop]._edges) {
            perSource[edge._src]++;
        }

        for (const auto& [source, count] : perSource) {
            EXPECT_LE(count, fanouts[hop])
                << "hop " << hop << " drew " << count << " rows for " << source;
        }
    }
}

TEST_F(GraphSAGESamplerTest, noPairRepeatsBeyondItsEdgeCount) {
    const ColumnNodeIDs seeds = {0, 1};
    const GraphSAGESampler::Fanouts fanouts {10, 10, 10};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 37, rows, stats);

    for (size_t hop = 0; hop < hops; hop++) {
        std::map<SampledEdge, size_t> drawn;
        for (const SampledEdge& edge : rows[hop]._edges) {
            drawn[edge]++;
        }

        for (const auto& [edge, count] : drawn) {
            EXPECT_LE(count, incidenceCount(edge))
                << "hop " << hop << " drew " << edge._src << " -> " << edge._tgt << " "
                << count << " times, but " << incidenceCount(edge) << " edges join them";
        }
    }
}

TEST_F(GraphSAGESamplerTest, noStepExceedsItsRowBudget) {
    const ColumnNodeIDs seeds = {0, 1, 8, 9, 11, 12, 15, 17};
    const std::vector<size_t> budgets {1, 2, 3, 4, 5, 7, 8, 16, 64, ChunkConfig::CHUNK_SIZE};
    const std::vector<size_t> widths {1, 2, 3, 4};

    for (const size_t budget : budgets) {
        for (const size_t width : widths) {
            if (budget < width) {
                continue;
            }

            const GraphSAGESampler::Fanouts fanouts {width, width, width};

            RunRows rows;
            RunStats stats;
            runSample(seeds, fanouts, budget, 41, rows, stats);

            EXPECT_LE(stats._widestStep, budget)
                << "budget " << budget << " fanout " << width << " left "
                << stats._widestStep << " rows in a step";
        }
    }
}

TEST_F(GraphSAGESamplerTest, unevenFanoutsRespectTheRowBudget) {
    const ColumnNodeIDs seeds = {0, 8, 9, 12};
    const std::vector<GraphSAGESampler::Fanouts> shapes {
        {1, 2, 3},
        {3, 2, 1},
        {1, 4, 2},
        {4, 1, 1},
        {2, 2, 4},
    };

    for (const GraphSAGESampler::Fanouts& fanouts : shapes) {
        const size_t widest = *std::ranges::max_element(fanouts);

        for (size_t budget = widest; budget <= widest + 6; budget++) {
            RunRows rows;
            RunStats stats;
            runSample(seeds, fanouts, budget, 43, rows, stats);

            EXPECT_LE(stats._widestStep, budget)
                << "fanouts " << fanouts[0] << "," << fanouts[1] << "," << fanouts[2]
                << " overran budget " << budget;
        }
    }
}

TEST_F(GraphSAGESamplerTest, budgetEqualToFanoutStillAdvances) {
    const ColumnNodeIDs seeds = {0, 8, 9, 11};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, 2, 2, rows, stats);

    EXPECT_GT(stats._steps, 1U) << "one node per step should take several steps";
    EXPECT_LE(stats._widestStep, 2U);
    EXPECT_FALSE(rows[0]._edges.empty());
}

TEST_F(GraphSAGESamplerTest, chunkSizeDoesNotChangeTheSample) {
    const ColumnNodeIDs seeds = {0, 1, 8, 9, 11};
    const GraphSAGESampler::Fanouts fanouts {3, 3, 3};
    constexpr size_t rngSeed = 47;

    RunRows whole;
    RunStats wholeStats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, rngSeed, whole, wholeStats);

    for (const size_t budget : {3, 4, 5, 9, 32}) {
        RunRows stepped;
        RunStats steppedStats;
        runSample(seeds, fanouts, budget, rngSeed, stepped, steppedStats);

        for (size_t hop = 0; hop < hops; hop++) {
            std::vector<SampledEdge> wholeEdges = whole[hop]._edges;
            std::vector<SampledEdge> steppedEdges = stepped[hop]._edges;
            std::ranges::sort(wholeEdges);
            std::ranges::sort(steppedEdges);

            EXPECT_EQ(wholeEdges, steppedEdges)
                << "hop " << hop << " sampled different edges at budget " << budget;

            std::vector<uint64_t> wholeNodes = whole[hop]._dstNodes;
            std::vector<uint64_t> steppedNodes = stepped[hop]._dstNodes;
            std::ranges::sort(wholeNodes);
            std::ranges::sort(steppedNodes);

            EXPECT_EQ(wholeNodes, steppedNodes)
                << "hop " << hop << " built a different frontier at budget " << budget;
        }
    }
}

TEST_F(GraphSAGESamplerTest, sameSeedGivesTheSameSample) {
    const ColumnNodeIDs seeds = {0, 8, 9, 11, 15};
    const GraphSAGESampler::Fanouts fanouts {2, 3, 2};

    RunRows first;
    RunStats firstStats;
    runSample(seeds, fanouts, 8, 53, first, firstStats);

    RunRows second;
    RunStats secondStats;
    runSample(seeds, fanouts, 8, 53, second, secondStats);

    EXPECT_EQ(firstStats._steps, secondStats._steps);

    for (size_t hop = 0; hop < hops; hop++) {
        EXPECT_EQ(first[hop]._edges, second[hop]._edges) << "hop " << hop << " edges differ";
        EXPECT_EQ(first[hop]._dstNodes, second[hop]._dstNodes)
            << "hop " << hop << " frontier differs";
    }
}

TEST_F(GraphSAGESamplerTest, differentSeedsDrawDifferently) {
    const ColumnNodeIDs seeds = {0, 1, 8, 9, 11, 12, 15, 17};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    RunRows first;
    RunStats firstStats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 101, first, firstStats);

    RunRows second;
    RunStats secondStats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 202, second, secondStats);

    bool anyDifference = false;
    for (size_t hop = 0; hop < hops; hop++) {
        if (first[hop]._edges != second[hop]._edges) {
            anyDifference = true;
        }
    }

    EXPECT_TRUE(anyDifference) << "two RNG seeds drew the identical sample";
}

TEST_F(GraphSAGESamplerTest, anUnknownSeedFinishesWithNoEdgeRows) {
    const ColumnNodeIDs seeds = {9999};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 61, rows, stats);

    EXPECT_EQ(stats._steps, 1U);

    for (size_t hop = 0; hop < hops; hop++) {
        EXPECT_TRUE(rows[hop]._edges.empty()) << "hop " << hop << " emitted an edge";
    }
}

TEST_F(GraphSAGESamplerTest, aSeedWithOnlyInEdgesIsSampled) {
    const ColumnNodeIDs seeds = {2};
    const GraphSAGESampler::Fanouts fanouts {4, 4, 4};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 67, rows, stats);

    const std::vector<uint64_t> expectedFrontier {2};
    EXPECT_EQ(rows[0]._dstNodes, expectedFrontier);

    std::vector<SampledEdge> edges = rows[0]._edges;
    std::ranges::sort(edges);

    const std::vector<SampledEdge> expected {{2, 0}, {2, 9}};
    EXPECT_EQ(edges, expected);
}

TEST_F(GraphSAGESamplerTest, aZeroFirstFanoutEmitsOnlyTheSeeds) {
    const ColumnNodeIDs seeds = {0, 8};
    const GraphSAGESampler::Fanouts fanouts {0, 2, 2};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 71, rows, stats);

    EXPECT_EQ(stats._steps, 1U);
    EXPECT_TRUE(rows[0]._edges.empty());
    EXPECT_TRUE(rows[1]._dstNodes.empty());
    EXPECT_TRUE(rows[2]._dstNodes.empty());

    std::vector<uint64_t> frontier = rows[0]._dstNodes;
    std::ranges::sort(frontier);

    const std::vector<uint64_t> expected {0, 8};
    EXPECT_EQ(frontier, expected);
}

TEST_F(GraphSAGESamplerTest, aZeroFanoutStopsTheRemainingHops) {
    const ColumnNodeIDs seeds = {0};
    const GraphSAGESampler::Fanouts fanouts {3, 0, 3};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 73, rows, stats);

    EXPECT_FALSE(rows[0]._edges.empty());
    EXPECT_TRUE(rows[1]._edges.empty());
    EXPECT_TRUE(rows[2]._dstNodes.empty());
}

TEST_F(GraphSAGESamplerTest, oneSeedReachesAllThreeHops) {
    const ColumnNodeIDs seeds = {0};
    const GraphSAGESampler::Fanouts fanouts {4, 4, 4};

    RunRows rows;
    RunStats stats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, 89, rows, stats);

    for (size_t hop = 0; hop < hops; hop++) {
        EXPECT_FALSE(rows[hop]._dstNodes.empty()) << "hop " << hop << " has no frontier";
        EXPECT_FALSE(rows[hop]._edges.empty()) << "hop " << hop << " emitted no edge";
    }
}

TEST_F(GraphSAGESamplerTest, resetAndReseedRepeatsTheRun) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const ColumnNodeIDs seeds = {0, 8, 9};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};
    constexpr size_t rngSeed = 79;

    std::array<NodeCol, hops> srcs {};
    std::array<NodeCol, hops> tgts {};
    std::array<NodeCol, hops> dstNodes {};

    GraphSAGESampler sampler(reader.getView(), rngSeed);
    for (size_t hop = 0; hop < hops; hop++) {
        sampler.setHopData(hop, &srcs[hop], &tgts[hop], &dstNodes[hop], fanouts[hop]);
    }

    const auto drive = [&](RunRows& rows) {
        rows = RunRows {};
        sampler.seed(&seeds);

        while (!sampler.finished()) {
            sampler.sample(4);

            for (size_t hop = 0; hop < hops; hop++) {
                collectHop(srcs[hop], tgts[hop], dstNodes[hop], rows[hop]);
            }
        }
    };

    RunRows first;
    drive(first);

    sampler.reset();

    RunRows second;
    drive(second);

    for (size_t hop = 0; hop < hops; hop++) {
        std::vector<uint64_t> firstNodes = first[hop]._dstNodes;
        std::vector<uint64_t> secondNodes = second[hop]._dstNodes;
        std::ranges::sort(firstNodes);
        std::ranges::sort(secondNodes);

        EXPECT_EQ(firstNodes, secondNodes) << "hop " << hop << " frontier not rebuilt";
    }
}

TEST_F(GraphSAGESamplerTest, everyBudgetProducesRectangularSteps) {
    const ColumnNodeIDs seeds = {0, 1, 8, 9, 11, 12, 15, 17};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};

    static_assert(hops * columnsPerHop == 9);

    for (const size_t budget : {2, 3, 6, 11, 64}) {
        RunRows rows;
        RunStats stats;
        runSample(seeds, fanouts, budget, 83, rows, stats);

        EXPECT_GT(stats._steps, 0U) << "budget " << budget << " produced no step";
    }
}

TEST_F(GraphSAGESamplerTest, unwiredColumnsAreSkipped) {
    const ColumnNodeIDs seeds = {0, 8, 12};
    const GraphSAGESampler::Fanouts fanouts {2, 2, 2};
    constexpr size_t rngSeed = 97;

    RunRows full;
    RunStats fullStats;
    runSample(seeds, fanouts, ChunkConfig::CHUNK_SIZE, rngSeed, full, fullStats);

    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    NodeCol firstDstNodes;
    NodeCol lastTgts;

    GraphSAGESampler sampler(reader.getView(), rngSeed);
    sampler.setHopData(0, nullptr, nullptr, &firstDstNodes, fanouts[0]);
    sampler.setHopData(1, nullptr, nullptr, nullptr, fanouts[1]);
    sampler.setHopData(2, nullptr, &lastTgts, nullptr, fanouts[2]);

    sampler.seed(&seeds);

    std::vector<uint64_t> dstNodes;
    std::vector<uint64_t> targets;
    size_t steps = 0;

    while (!sampler.finished()) {
        sampler.sample(ChunkConfig::CHUNK_SIZE);
        steps++;

        ASSERT_LE(steps, fullStats._steps) << "the wired run outlasted the full one";

        for (const std::optional<NodeID>& node : firstDstNodes) {
            if (node.has_value()) {
                dstNodes.push_back(node->getValue());
            }
        }

        for (const std::optional<NodeID>& node : lastTgts) {
            if (node.has_value()) {
                targets.push_back(node->getValue());
            }
        }
    }

    EXPECT_EQ(steps, fullStats._steps);
    EXPECT_EQ(dstNodes, full[0]._dstNodes);

    std::vector<uint64_t> expectedTargets;
    for (const SampledEdge& edge : full[2]._edges) {
        expectedTargets.push_back(edge._tgt);
    }

    EXPECT_EQ(targets, expectedTargets);
}
