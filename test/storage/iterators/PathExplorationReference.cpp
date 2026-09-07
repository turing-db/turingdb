#include "PathExplorationReference.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <stdexcept>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnVector.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorator.h"
#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "list/PathTrie.h"

using namespace db;
using namespace turing::test;

void turing::test::buildAdjacency(const GraphView& view, size_t nodeCount, Adjacency& adjacency) {
    ColumnNodeIDs input;
    for (size_t node = 0; node < nodeCount; node++) {
        input.push_back(NodeID(node));
    }

    adjacency._outs.assign(nodeCount, {});
    adjacency._ins.assign(nodeCount, {});

    ColumnVector<size_t> indices;
    ColumnEdgeIDs edgeIDs;
    ColumnNodeIDs others;
    ColumnEdgeTypes types;

    GetOutEdgesChunkWriter outWriter(view, &input);
    outWriter.setIndices(&indices);
    outWriter.setEdgeIDs(&edgeIDs);
    outWriter.setTgtIDs(&others);
    outWriter.setEdgeTypes(&types);

    while (outWriter.isValid()) {
        outWriter.fill(ChunkConfig::CHUNK_SIZE);
        for (size_t row = 0; row < indices.size(); row++) {
            adjacency._outs[indices[row]].push_back({edgeIDs[row].getValue(), others[row].getValue(), types[row].getValue()});
        }
    }

    GetInEdgesChunkWriter inWriter(view, &input);
    inWriter.setIndices(&indices);
    inWriter.setEdgeIDs(&edgeIDs);
    inWriter.setSrcIDs(&others);
    inWriter.setEdgeTypes(&types);

    while (inWriter.isValid()) {
        inWriter.fill(ChunkConfig::CHUNK_SIZE);
        for (size_t row = 0; row < indices.size(); row++) {
            adjacency._ins[indices[row]].push_back({edgeIDs[row].getValue(), others[row].getValue(), types[row].getValue()});
        }
    }
}

uint64_t turing::test::edgeBetween(const Adjacency& adjacency, uint64_t source, uint64_t target) {
    for (const ReferenceEdge& edge : adjacency._outs[source]) {
        if (edge._other == target) {
            return edge._edge;
        }
    }

    throw std::runtime_error("No such edge in the fixture");
}

ReferenceEnumerator::ReferenceEnumerator(const Adjacency& adjacency,
                                         PathExplorationDir direction,
                                         uint64_t minHops,
                                         uint64_t maxHops)
    : _adjacency(adjacency),
    _direction(direction),
    _minHops(minHops),
    _maxHops(maxHops)
{
}

void ReferenceEnumerator::enumerate(const ColumnNodeIDs& seeds, std::vector<PathRow>& rows) {
    rows.clear();
    std::vector<uint64_t> path;
    for (size_t row = 0; row < seeds.size(); row++) {
        walk(row, seeds[row].getValue(), path, rows);
    }
}

void ReferenceEnumerator::walk(size_t seedRow, uint64_t node, std::vector<uint64_t>& path, std::vector<PathRow>& rows) {
    const uint64_t depth = path.size();
    const bool ends = !_ends || (*_ends)[node];
    if (depth >= _minHops && ends) {
        rows.push_back({seedRow, node, path});
    }

    if (depth >= _maxHops) {
        return;
    }

    if (_direction != PathExplorationDir::BACKWARD) {
        descend(seedRow, node, _adjacency._outs[node], path, rows);
    }

    if (_direction != PathExplorationDir::FORWARD) {
        descend(seedRow, node, _adjacency._ins[node], path, rows);
    }
}

void ReferenceEnumerator::descend(size_t seedRow,
                                  uint64_t node,
                                  const std::vector<ReferenceEdge>& candidates,
                                  std::vector<uint64_t>& path,
                                  std::vector<PathRow>& rows) {
    for (const ReferenceEdge& candidate : candidates) {
        const bool wrongType = _edgeType && candidate._type != *_edgeType;
        const bool onTrail = std::find(path.begin(), path.end(), candidate._edge) != path.end();
        const bool rejected = _predicate && !_predicate(node, candidate._edge, candidate._other);
        if (wrongType || onTrail || rejected) {
            continue;
        }

        path.push_back(candidate._edge);
        walk(seedRow, candidate._other, path, rows);
        path.pop_back();
    }
}

PredicateHopFilter::PredicateHopFilter(HopPredicate predicate)
    : _predicate(predicate)
{
}

PredicateHopFilter::~PredicateHopFilter() {
}

size_t PredicateHopFilter::filter(NodeID source, std::span<NodeID> nodes, std::span<EdgeID> edges) {
    size_t kept = 0;
    for (size_t candidate = 0; candidate < edges.size(); candidate++) {
        if (_predicate(source.getValue(), edges[candidate].getValue(), nodes[candidate].getValue())) {
            nodes[kept] = nodes[candidate];
            edges[kept] = edges[candidate];
            kept++;
        }
    }

    return kept;
}

size_t turing::test::collectPaths(const GraphView& view,
                                  const ColumnNodeIDs& input,
                                  PathExplorationDir direction,
                                  uint64_t minHops,
                                  uint64_t maxHops,
                                  const ExplorationOptions& options,
                                  std::vector<PathRow>& rows) {
    ColumnVector<size_t> indices;
    ColumnNodeIDs targets;
    ColumnVector<PathRef> paths;
    PathTrie trie;
    ListBuffer<> buffer;

    PathExplorator explorator(view, &input, direction, minHops, maxHops);
    explorator.setIndices(&indices);
    if (options._collectTargets) {
        explorator.setTargets(&targets);
    }
    if (options._collectPaths) {
        explorator.setPaths(&paths, &trie);
    }
    if (options._edgeType) {
        explorator.setEdgeTypeFilter(*options._edgeType);
    }
    explorator.setHopFilter(options._hopFilter);
    explorator.setEndLabels(options._endLabels);
    explorator.setDistanceIndex(options._distanceIndex);
    explorator.setWalkerCount(options._walkerCount);
    explorator.setCandidateLookahead(options._lookahead);

    rows.clear();
    while (explorator.isValid()) {
        explorator.fill(options._maxCount);

        for (size_t row = 0; row < indices.size(); row++) {
            PathRow& emitted = rows.emplace_back();
            emitted._index = indices[row];
            emitted._target = options._collectTargets ? targets[row].getValue() : 0;

            if (options._collectPaths) {
                const ListView edges = trie.expandEdges(paths[row], buffer);
                for (const ListElementView& element : edges) {
                    emitted._edges.push_back(element.getAs<EdgeID>().getValue());
                }

                std::vector<uint64_t> sorted = emitted._edges;
                std::sort(sorted.begin(), sorted.end());
                EXPECT_EQ(std::adjacent_find(sorted.begin(), sorted.end()), sorted.end())
                    << "An edge appears twice on one path";
            }
        }
    }

    return explorator.getCandidateCheckCount();
}

void turing::test::expectSameRows(std::vector<PathRow> expected, std::vector<PathRow> actual) {
    std::sort(expected.begin(), expected.end());
    std::sort(actual.begin(), actual.end());
    EXPECT_EQ(expected, actual);
}

size_t turing::test::countRowsThrough(const std::vector<PathRow>& rows, uint64_t edge) {
    size_t count = 0;
    for (const PathRow& row : rows) {
        if (std::find(row._edges.begin(), row._edges.end(), edge) != row._edges.end()) {
            count++;
        }
    }

    return count;
}
