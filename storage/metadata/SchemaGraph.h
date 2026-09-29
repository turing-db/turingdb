#pragma once

#include <mutex>
#include <span>
#include <stddef.h>
#include <vector>

#include "datapart/DataPartSpan.h"
#include "metadata/LabelSet.h"
#include "ID.h"

namespace db {

class LabelSetMap;

// The edges of one type from the nodes of one label set to the nodes of another, and how
// many of them close on their own node
struct SchemaArc {
    LabelSetID _source {0};
    EdgeTypeID _type {0};
    LabelSetID _target {0};
    size_t _count {0};
    size_t _selfLoopCount {0};
};

// A pattern to embed in the summary: each node carries at least its labels, each edge
// carries one of its types, any type when none is listed, and an undirected edge reads
// either way
struct SchemaPattern {
    struct Node {
        LabelSet _labels;
    };

    struct Edge {
        size_t _source {0};
        size_t _target {0};
        std::vector<EdgeTypeID> _types;
        bool _undirected {false};
    };

    std::vector<Node> _nodes;
    std::vector<Edge> _edges;
};

// The graph summarised by label set and edge type: one arc per (source label set, edge
// type, target label set) the parts hold, counted over every edge including the deleted
// ones, so an arc the summary lacks the data lacks. Built on first use and again for a
// view holding more parts than it was built on.
class SchemaGraph {
public:
    SchemaGraph();
    ~SchemaGraph();

    SchemaGraph(const SchemaGraph&) = delete;
    SchemaGraph& operator=(const SchemaGraph&) = delete;

    void refresh(DataPartSpan parts);

    std::span<const SchemaArc> arcs() const { return _arcs; }

    // Whether the pattern maps homomorphically into the summary: every node onto a label
    // set carrying its labels, every edge onto an arc of one of its types between the two,
    // and an edge closing on its own node onto an arc some self-loop holds. A search that
    // outgrows its budget answers true.
    bool embeds(const SchemaPattern& pattern, const LabelSetMap& labelSets) const;

private:
    mutable std::mutex _mutex;
    bool _built {false};
    size_t _nodeCount {0};
    size_t _edgeCount {0};
    std::vector<SchemaArc> _arcs;

    void build(DataPartSpan parts);
};

}
