#pragma once

#include <stddef.h>
#include <span>
#include <vector>

#include "ID.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "metadata/LabelSet.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLColumnFunctions.h"

namespace db {

// The seed-expansion core shared by every loop that fans an input node chunk out: the
// input, the seed and end node chunks a step fills, the row-to-input-row indices that
// gather the carry set, the limit and the body
class NLExpansionLoopData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    NLExpansionLoopData(const ColumnNodeIDs* input,
                        ColumnNodeIDs* sources,
                        ColumnNodeIDs* targets)
        : _inputNodeIDs(input),
        _sources(sources),
        _targets(targets)
    {
    }

    const ColumnNodeIDs* getInput() const { return _inputNodeIDs; }

    ColumnNodeIDs* getSources() const { return _sources; }
    ColumnNodeIDs* getTargets() const { return _targets; }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

private:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    NLLimitState* _limit {nullptr};

    ColumnNodeIDs* _sources {nullptr};
    ColumnNodeIDs* _targets {nullptr};

    CarriedColumns _carriedColumns;
    NLStmtContainer _stmts;

    // Scratch for the writer's row-to-input-row map, which drives the gathers
    ColumnVector<size_t> _indices;
};

// nl.get_out_edges and nl.get_in_edges loop data
// The state is the same for get_out_edges/get_in_edges
class NLEdgeLoopData : public NLExpansionLoopData {
public:
    NLEdgeLoopData(const ColumnNodeIDs* input,
                   ColumnNodeIDs* sources,
                   ColumnEdgeIDs* edgeIDs,
                   ColumnEdgeTypes* edgeTypes,
                   ColumnNodeIDs* targets)
        : NLExpansionLoopData(input, sources, targets),
        _edgeIDs(edgeIDs),
        _edgeTypes(edgeTypes)
    {
    }

    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }

private:
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
};

// nl.get_out_edges_by_type / nl.get_in_edges_by_type loop data: an edge hop
// restricted to the types the pattern named. Reuses the plain edge hop's chunks, limit, carry
// set and body; adds the resolved EdgeTypeIDs the Get{Out,In}EdgesByTypeChunkWriter
// filters by, owned here so the span it walks each run points at storage that outlives
// it. A name absent from the schema resolves to nothing, so an empty set matches no
// edge and the loop emits no row - the edge sibling of NLScanByLabelLoopData.
class NLEdgeByTypeLoopData : public NLEdgeLoopData {
public:
    NLEdgeByTypeLoopData(const ColumnNodeIDs* input,
                         ColumnNodeIDs* sources,
                         ColumnEdgeIDs* edgeIDs,
                         ColumnEdgeTypes* edgeTypes,
                         ColumnNodeIDs* targets,
                         std::span<const EdgeTypeID> requestedTypes)
        : NLEdgeLoopData(input, sources, edgeIDs, edgeTypes, targets),
        _requestedTypes(requestedTypes.begin(), requestedTypes.end())
    {
    }

    std::span<const EdgeTypeID> getRequestedTypes() const { return _requestedTypes; }
    bool isMatchable() const { return !_requestedTypes.empty(); }

private:
    std::vector<EdgeTypeID> _requestedTypes;
};

// nl.get_out_edges_by_label / nl.get_in_edges_by_label loop data: an edge hop restricted to
// the edges whose far endpoint carries a label set - the target of an out-edge, the source
// of an in-edge. Reuses the plain edge hop's chunks, limit, carry set and body; adds the
// label set the Get{Out,In}EdgesByLabelChunkWriter filters by, owned here so the
// LabelSetHandle the executor builds each run points at storage that outlives it.
// _matchable is false when a requested label was absent from the schema, leaving the
// conjunction unsatisfiable, so the loop emits no row.
class NLEdgeByLabelLoopData : public NLEdgeLoopData {
public:
    NLEdgeByLabelLoopData(const ColumnNodeIDs* input,
                          ColumnNodeIDs* sources,
                          ColumnEdgeIDs* edgeIDs,
                          ColumnEdgeTypes* edgeTypes,
                          ColumnNodeIDs* targets,
                          const LabelSet& labelset,
                          bool matchable)
        : NLEdgeLoopData(input, sources, edgeIDs, edgeTypes, targets),
        _labelset(labelset),
        _matchable(matchable)
    {
    }

    const LabelSet& getLabelSet() const { return _labelset; }
    bool isMatchable() const { return _matchable; }

private:
    LabelSet _labelset;
    bool _matchable {true};
};

// nl.get_out_edges_by_type_and_label / nl.get_in_edges_by_type_and_label loop data: the
// requested types of NLEdgeByTypeLoopData and the label set of NLEdgeByLabelLoopData
// together. The loop emits no row when no type resolved or a label is absent.
class NLEdgeByTypeAndLabelLoopData : public NLEdgeLoopData {
public:
    NLEdgeByTypeAndLabelLoopData(const ColumnNodeIDs* input,
                                 ColumnNodeIDs* sources,
                                 ColumnEdgeIDs* edgeIDs,
                                 ColumnEdgeTypes* edgeTypes,
                                 ColumnNodeIDs* targets,
                                 std::span<const EdgeTypeID> requestedTypes,
                                 const LabelSet& labelset,
                                 bool labelsMatchable)
        : NLEdgeLoopData(input, sources, edgeIDs, edgeTypes, targets),
        _requestedTypes(requestedTypes.begin(), requestedTypes.end()),
        _labelset(labelset),
        _labelsMatchable(labelsMatchable)
    {
    }

    std::span<const EdgeTypeID> getRequestedTypes() const { return _requestedTypes; }
    const LabelSet& getLabelSet() const { return _labelset; }
    bool isMatchable() const { return _labelsMatchable && !_requestedTypes.empty(); }

private:
    std::vector<EdgeTypeID> _requestedTypes;
    LabelSet _labelset;
    bool _labelsMatchable {true};
};

}
