#include <algorithm>
#include <memory>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
#include "iterators/ScanEdgesByTypeIterator.h"
#include "iterators/ScanEdgesIterator.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "FileUtils.h"
#include "JobSystem.h"

using namespace db;
using namespace turing::test;

namespace {

// One emitted edge row, flattened to raw values so the rows compare without
// depending on the ID types' operators.
struct ScannedEdge {
    uint64_t _source {0};
    uint64_t _edgeID {0};
    uint64_t _target {0};
    uint64_t _type {0};

    bool operator==(const ScannedEdge& other) const = default;
};

// Drive a by-type edge scan to exhaustion, gathering one row per emitted edge.
// maxCount is the per-fill row budget: a small value exercises the mid-span
// resume path across successive fill() calls.
void collectEdgesByType(const GraphReader& reader,
                        EdgeTypeID edgeType,
                        size_t maxCount,
                        std::vector<ScannedEdge>& out) {
    ColumnNodeIDs sources;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;
    ColumnEdgeTypes types;

    ScanEdgesByTypeChunkWriter writer(reader.getView(), edgeType);
    writer.setSrcIDs(&sources);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);
    writer.setEdgeTypes(&types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);

        for (size_t i = 0; i < sources.size(); i++) {
            out.push_back({sources[i].getValue(), edgeIDs[i].getValue(), targets[i].getValue(), types[i].getValue()});
        }
    }
}

// Gather every edge via the unfiltered scan - ground truth the by-type scan is
// compared against once filtered to a single type.
void collectAllEdges(const GraphReader& reader, std::vector<ScannedEdge>& out) {
    ColumnNodeIDs sources;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs targets;
    ColumnEdgeTypes types;

    ScanEdgesChunkWriter writer(reader.getView());
    writer.setSrcIDs(&sources);
    writer.setEdgeIDs(&edgeIDs);
    writer.setTgtIDs(&targets);
    writer.setEdgeTypes(&types);

    out.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);

        for (size_t i = 0; i < sources.size(); i++) {
            out.push_back({sources[i].getValue(), edgeIDs[i].getValue(), targets[i].getValue(), types[i].getValue()});
        }
    }
}

// Keep only the rows whose edge type matches, preserving order.
void filterByType(const std::vector<ScannedEdge>& all,
                  EdgeTypeID edgeType,
                  std::vector<ScannedEdge>& out) {
    out.clear();
    for (const ScannedEdge& edge : all) {
        if (edge._type == edgeType.getValue()) {
            out.push_back(edge);
        }
    }
}

// Compare rows by (source, target) as an order-independent multiset, so a test can
// pin the emitted content without hard-coding the edge-span traversal order.
void expectSameContent(std::vector<std::pair<uint64_t, uint64_t>> expected,
                       const std::vector<ScannedEdge>& actual) {
    std::vector<std::pair<uint64_t, uint64_t>> actualPairs;
    for (const ScannedEdge& edge : actual) {
        actualPairs.emplace_back(edge._source, edge._target);
    }

    std::sort(expected.begin(), expected.end());
    std::sort(actualPairs.begin(), actualPairs.end());

    EXPECT_EQ(expected, actualPairs);
}

}

class ScanEdgesByTypeIteratorTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        auto change = _graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID person = metadata.getOrCreateLabel("Person");
        const LabelSet labelset = LabelSet::fromList({person});

        _knows = metadata.getOrCreateEdgeType("KNOWS");
        _likes = metadata.getOrCreateEdgeType("LIKES");
        _worksAt = metadata.getOrCreateEdgeType("WORKS_AT");

        // Registered but never attached to an edge: an existing type that matches
        // nothing.
        _follows = metadata.getOrCreateEdgeType("FOLLOWS");

        // Single datapart, so the temporary IDs 0..5 are also the final node IDs.
        for (size_t i = 0; i < 6; i++) {
            builder.addNode(labelset);
        }

        // A mix of types per source node, so filtering has something to keep and
        // something to drop at every node.
        builder.addEdge(_knows, 0, 1);
        builder.addEdge(_likes, 0, 2);
        builder.addEdge(_knows, 0, 3);
        builder.addEdge(_likes, 1, 2);
        builder.addEdge(_worksAt, 2, 3);
        builder.addEdge(_knows, 4, 5);
        builder.addEdge(_likes, 4, 0);
        builder.addEdge(_knows, 5, 0);
        builder.addEdge(_worksAt, 5, 1);

        const auto res = change->access().submit(*_jobSystem);
        if (!res) {
            spdlog::error("Failed to submit change: {}", res.error().fmtMessage());
        }
        ASSERT_TRUE(res);
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    std::unique_ptr<db::JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph = nullptr;

    EdgeTypeID _knows;
    EdgeTypeID _likes;
    EdgeTypeID _worksAt;
    EdgeTypeID _follows;

    FileUtils::Path _logPath;
};

// For every registered edge type, the edges the by-type scan emits must equal the
// unfiltered scan filtered down to that type - same rows, same order.
TEST_F(ScanEdgesByTypeIteratorTest, byTypeScanMatchesFilteredFullScan) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<ScannedEdge> allEdges;
    collectAllEdges(reader, allEdges);

    for (const EdgeTypeID edgeType : {_knows, _likes, _worksAt, _follows}) {
        std::vector<ScannedEdge> expected;
        filterByType(allEdges, edgeType, expected);

        std::vector<ScannedEdge> actual;
        collectEdgesByType(reader, edgeType, ChunkConfig::CHUNK_SIZE, actual);

        EXPECT_EQ(expected, actual);
    }
}

// Hand-derived expected content, so the differential test above cannot be fooled
// by a matching bug in the unfiltered scan.
TEST_F(ScanEdgesByTypeIteratorTest, byTypeScanEmitsTheEdgesOfThatType) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<ScannedEdge> knowsEdges;
    collectEdgesByType(reader, _knows, ChunkConfig::CHUNK_SIZE, knowsEdges);
    expectSameContent({{0, 1}, {0, 3}, {4, 5}, {5, 0}}, knowsEdges);

    std::vector<ScannedEdge> likesEdges;
    collectEdgesByType(reader, _likes, ChunkConfig::CHUNK_SIZE, likesEdges);
    expectSameContent({{0, 2}, {1, 2}, {4, 0}}, likesEdges);

    std::vector<ScannedEdge> worksAtEdges;
    collectEdgesByType(reader, _worksAt, ChunkConfig::CHUNK_SIZE, worksAtEdges);
    expectSameContent({{2, 3}, {5, 1}}, worksAtEdges);
}

// A registered type no edge carries yields nothing rather than every edge.
TEST_F(ScanEdgesByTypeIteratorTest, unusedTypeEmitsNoRow) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<ScannedEdge> edges;
    collectEdgesByType(reader, _follows, ChunkConfig::CHUNK_SIZE, edges);

    EXPECT_TRUE(edges.empty());
}

// A row budget smaller than the match count makes each fill stop mid-span, so the
// scan has to resume from the edge it left off at rather than restart or skip.
TEST_F(ScanEdgesByTypeIteratorTest, byTypeScanResumesAcrossFills) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<ScannedEdge> wholeChunk;
    collectEdgesByType(reader, _knows, ChunkConfig::CHUNK_SIZE, wholeChunk);

    for (const size_t budget : {1u, 2u, 3u}) {
        std::vector<ScannedEdge> chunked;
        collectEdgesByType(reader, _knows, budget, chunked);

        EXPECT_EQ(wholeChunk, chunked);
    }
}
