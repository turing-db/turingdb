#pragma once

#include <stddef.h>
#include <stdint.h>
#include <concepts>
#include <span>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "ID.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"
#include "NLLimitState.h"
#include "NLProgramRows.h"

namespace db {

class NLExecutionContext;

// nl.scan_nodes loop data
class NLScanLoopData : public NLFunctionData {
public:
    NLScanLoopData(ColumnNodeIDs* nodeIDs)
        : _nodeIDs(nodeIDs)
    {
    }

    ColumnNodeIDs* getNodeIDs() const { return _nodeIDs; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    ColumnNodeIDs* _nodeIDs {nullptr};
    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// nl.scan_nodes_by_label loop data: a node scan restricted to the nodes whose
// label set is a superset of _labelset. Reuses the plain scan's node chunk,
// limit and body; adds the resolved label set the ScanNodesByLabelChunkWriter
// filters by.
class NLScanByLabelLoopData : public NLScanLoopData {
public:
    // The label set is owned here so the LabelSetHandle the executor builds each
    // run points at storage that outlives it (this data lives for the whole
    // program). _matchable is false when a requested label was absent from the
    // schema, leaving the conjunction unsatisfiable, so the loop emits no row.
    NLScanByLabelLoopData(ColumnNodeIDs* nodeIDs, const LabelSet& labelset, bool matchable)
        : NLScanLoopData(nodeIDs),
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

// nl.const_scan_nodes loop data: a node scan over a fixed, explicitly listed set
// of node IDs rather than a walk of the graph. Reuses the plain scan's node chunk,
// limit and body; adds the owned list of node IDs the loop emits, one chunk-sized
// slice per step. The translator fills the list in place through addNodeID after
// allocating the data, so the IDs are never staged in a temporary; the list is
// owned here so it outlives the op's attribute storage (this data lives for the
// whole program).
class NLConstScanLoopData : public NLScanLoopData {
public:
    NLConstScanLoopData(ColumnNodeIDs* nodeIDs)
        : NLScanLoopData(nodeIDs)
    {
    }

    std::span<const NodeID> getConstNodeIDs() const { return _constNodeIDs; }

    // Reserve room for the list, then append each ID in turn: the translator fills the
    // list here directly rather than building and copying one.
    void reserveNodeIDs(size_t count) { _constNodeIDs.reserve(count); }
    void addNodeID(NodeID nodeID) { _constNodeIDs.push_back(nodeID); }

    // Each a constant column holding, nullable or not, a number or a list of them
    std::span<const Column* const> getBoundIDs() const { return _boundIDs; }
    bool hasBoundIDs() const { return !_boundIDs.empty(); }

    void addBoundIDs(const Column* column) { _boundIDs.push_back(column); }

    std::vector<NodeID>& getListedNodeIDs() { return _listedNodeIDs; }

private:
    std::vector<NodeID> _constNodeIDs;
    std::vector<const Column*> _boundIDs;

    // The IDs naming a node a scan produces, listed again each time the loop starts
    std::vector<NodeID> _listedNodeIDs;
};

// nl.scan_nodes_by_property_value loop data. The label set and the literal are owned here
// so the handle and the view the executor builds stay valid for every run; _matchable is
// false when the property or a label is absent from the schema, or when no stored value
// can equal the literal, and the loop then emits no row.
class NLScanByPropertyValueLoopData : public NLScanLoopData {
public:
    using Literal = std::variant<int64_t, uint64_t, double, CustomBool, std::string>;

    NLScanByPropertyValueLoopData(ColumnNodeIDs* nodeIDs,
                                  PropertyTypeID propertyTypeID,
                                  ValueType valueType,
                                  const Literal& literal,
                                  const LabelSet& labelset,
                                  bool byLabel,
                                  bool matchable)
        : NLScanLoopData(nodeIDs),
        _propertyTypeID(propertyTypeID),
        _valueType(valueType),
        _literal(literal),
        _labelset(labelset),
        _byLabel(byLabel),
        _matchable(matchable)
    {
    }

    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }
    ValueType getValueType() const { return _valueType; }
    const Literal& getLiteral() const { return _literal; }
    bool isMatchable() const { return _matchable; }
    const LabelSet& getLabelSet() const { return _labelset; }
    bool isByLabel() const { return _byLabel; }

    // The column holding the value bound when the query runs, which stands in for the
    // literal, and the type of that value; a null column when the query spells the literal
    const Column* getBoundValue() const { return _boundValue; }
    ValueType getBoundType() const { return _boundType; }
    void setBoundValue(const Column* boundValue, ValueType boundType);

    // Converts @param given to the @param stored type of the property. A number equals a
    // number of another type by value, so 32.0 finds an age of 32. Any other pairing - a
    // negative integer against UInt64, 32.5 against Int64, a string against a number -
    // converts to nothing, and the scan matches no node.
    static bool convertToStored(ValueType stored, const Literal& given, Literal& converted);

private:
    PropertyTypeID _propertyTypeID {};
    ValueType _valueType {ValueType::Invalid};
    Literal _literal;
    LabelSet _labelset;
    bool _byLabel {false};
    bool _matchable {false};
    const Column* _boundValue {nullptr};
    ValueType _boundType {ValueType::Invalid};
};

// nl.scan_edges loop data: the edge sibling of NLScanLoopData. A source loop
// (no input column, no carry set), so - unlike NLEdgeLoopData - it holds only
// the four fixed output chunks a step fills (sources, edge IDs, edge type IDs,
// targets), plus the limit and body a scan loop carries.
class NLScanEdgesLoopData : public NLFunctionData {
public:
    NLScanEdgesLoopData(ColumnNodeIDs* sources,
                        ColumnEdgeIDs* edgeIDs,
                        ColumnEdgeTypes* edgeTypes,
                        ColumnNodeIDs* targets)
        : _sources(sources),
        _edgeIDs(edgeIDs),
        _edgeTypes(edgeTypes),
        _targets(targets)
    {
    }

    ColumnNodeIDs* getSources() const { return _sources; }
    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }
    ColumnNodeIDs* getTargets() const { return _targets; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    // The four fixed chunks of an edge iterator step, in loop-variable order
    ColumnNodeIDs* _sources {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _targets {nullptr};

    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// nl.scan_edges_by_type loop data: the edge scan restricted to the types the pattern
// named. Reuses the plain edge scan's chunks, limit and body; adds the resolved
// EdgeTypeIDs the ScanEdgesByTypeChunkWriter filters by, owned here so the span it
// walks each run points at storage that outlives it. A name absent from the schema
// resolves to nothing, so an empty set matches no edge and the loop emits no row -
// the scan sibling of NLEdgeByTypeLoopData.
class NLScanEdgesByTypeLoopData : public NLScanEdgesLoopData {
public:
    NLScanEdgesByTypeLoopData(ColumnNodeIDs* sources,
                              ColumnEdgeIDs* edgeIDs,
                              ColumnEdgeTypes* edgeTypes,
                              ColumnNodeIDs* targets,
                              std::span<const EdgeTypeID> requestedTypes)
        : NLScanEdgesLoopData(sources, edgeIDs, edgeTypes, targets),
        _requestedTypes(requestedTypes.begin(), requestedTypes.end())
    {
    }

    std::span<const EdgeTypeID> getRequestedTypes() const { return _requestedTypes; }
    bool isMatchable() const { return !_requestedTypes.empty(); }

private:
    std::vector<EdgeTypeID> _requestedTypes;
};

// nl.const_scan_edges loop data: the edge scan over a fixed, explicitly listed set of edge
// IDs, the edge sibling of NLConstScanLoopData
class NLConstScanEdgesLoopData : public NLScanEdgesLoopData {
public:
    NLConstScanEdgesLoopData(ColumnNodeIDs* sources,
                             ColumnEdgeIDs* edgeIDs,
                             ColumnEdgeTypes* edgeTypes,
                             ColumnNodeIDs* targets,
                             bool undirected)
        : NLScanEdgesLoopData(sources, edgeIDs, edgeTypes, targets),
        _undirected(undirected)
    {
    }

    std::span<const EdgeID> getConstEdgeIDs() const { return _constEdgeIDs; }
    void reserveEdgeIDs(size_t count) { _constEdgeIDs.reserve(count); }
    void addEdgeID(EdgeID edgeID) { _constEdgeIDs.push_back(edgeID); }

    // Each a constant column holding, nullable or not, an edge, an integer or a list
    std::span<const Column* const> getBoundIDs() const { return _boundIDs; }
    void addBoundIDs(const Column* column) { _boundIDs.push_back(column); }

    bool isUndirected() const { return _undirected; }

    std::vector<EdgeID>& getListedEdgeIDs() { return _listedEdgeIDs; }
    std::vector<EdgeRecord>& getListedRows() { return _listedRows; }

private:
    std::vector<EdgeID> _constEdgeIDs;
    std::vector<const Column*> _boundIDs;
    bool _undirected {false};

    // The edges a scan produces, and the rows they come out as, listed again each time the
    // loop starts
    std::vector<EdgeID> _listedEdgeIDs;
    std::vector<EdgeRecord> _listedRows;
};

// Loop data of the four by-label edge scans: the edge scan restricted to the edges hanging
// off a node whose label set is a superset of _labelset - the ones leaving it where the
// source carries the labels, the ones arriving at it where the target does.
// The state is the same either way, so the four share it and differ only in the executor
// they run. Reuses the plain edge scan's chunks, limit and body; adds the resolved label
// set the by-label chunk writers filter by, owned here so the LabelSetHandle the executor
// builds each run points at storage that outlives it. _matchable is false when a requested
// label was absent from the schema, leaving the conjunction unsatisfiable, so the loop
// emits no row.
class NLScanEdgesByLabelLoopData : public NLScanEdgesLoopData {
public:
    NLScanEdgesByLabelLoopData(ColumnNodeIDs* sources,
                               ColumnEdgeIDs* edgeIDs,
                               ColumnEdgeTypes* edgeTypes,
                               ColumnNodeIDs* targets,
                               const LabelSet& labelset,
                               bool matchable)
        : NLScanEdgesLoopData(sources, edgeIDs, edgeTypes, targets),
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

// Loop data of the two both-ends edge scans: the by-label scan's, keyed by the label set
// of the end whose index it reads, with the label set the other end must carry.
class NLScanEdgesByEndpointLabelsLoopData : public NLScanEdgesByLabelLoopData {
public:
    NLScanEdgesByEndpointLabelsLoopData(ColumnNodeIDs* sources,
                                        ColumnEdgeIDs* edgeIDs,
                                        ColumnEdgeTypes* edgeTypes,
                                        ColumnNodeIDs* targets,
                                        const LabelSet& indexedLabelSet,
                                        const LabelSet& otherEndLabelSet,
                                        bool matchable)
        : NLScanEdgesByLabelLoopData(sources, edgeIDs, edgeTypes, targets, indexedLabelSet, matchable),
        _otherEndLabelSet(otherEndLabelSet)
    {
    }

    const LabelSet& getOtherEndLabelSet() const { return _otherEndLabelSet; }

private:
    LabelSet _otherEndLabelSet;
};

// Type of handle that appends the index of every row whose ID cell names a node a scan
// would produce, and that node. One per kind of cell an ID is read from.
using NLFetchNodesFunction = void (*)(const Column* ids,
                                      const NLExecutionContext* context,
                                      ColumnVector<size_t>* indices,
                                      ColumnNodeIDs* nodes);

class NLFetchNodesData : public NLFunctionData {
public:
    NLFetchNodesData(const Column* ids, NLFetchNodesFunction fetch, ColumnNodeIDs* nodes)
        : _ids(ids),
        _fetch(fetch),
        _nodes(nodes)
    {
    }

    const Column* getIDs() const { return _ids; }
    NLFetchNodesFunction getFetch() const { return _fetch; }
    ColumnNodeIDs* getNodes() const { return _nodes; }

    const std::vector<NLFilterData::FilterColumn>& getColumns() const { return _columns; }

    void addColumn(const NLFilterData::FilterColumn& column) {
        _columns.push_back(column);
    }

    ColumnVector<size_t>* getIndices() { return &_indices; }

private:
    const Column* _ids {nullptr};
    NLFetchNodesFunction _fetch {nullptr};
    ColumnNodeIDs* _nodes {nullptr};
    std::vector<NLFilterData::FilterColumn> _columns;
    ColumnVector<size_t> _indices;
};

class NLFetchEdgesData;

// The edge sibling of NLFetchNodesFunction, appending the source, edge, type and target of
// each row's edge
using NLFetchEdgesFunction = void (*)(const NLExecutionContext* context, NLFetchEdgesData* fetch);

template <TypedInternalID IDType>
using NLFetchFunction = std::conditional_t<std::same_as<IDType, NodeID>, NLFetchNodesFunction, NLFetchEdgesFunction>;

class NLFetchEdgesData : public NLFunctionData {
public:
    NLFetchEdgesData(const Column* ids,
                     NLFetchEdgesFunction fetch,
                     bool undirected,
                     ColumnNodeIDs* sources,
                     ColumnEdgeIDs* edgeIDs,
                     ColumnEdgeTypes* edgeTypes,
                     ColumnNodeIDs* targets)
        : _ids(ids),
        _fetch(fetch),
        _undirected(undirected),
        _sources(sources),
        _edgeIDs(edgeIDs),
        _edgeTypes(edgeTypes),
        _targets(targets)
    {
    }

    const Column* getIDs() const { return _ids; }
    NLFetchEdgesFunction getFetch() const { return _fetch; }
    bool isUndirected() const { return _undirected; }

    ColumnNodeIDs* getSources() const { return _sources; }
    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }
    ColumnNodeIDs* getTargets() const { return _targets; }

    const std::vector<NLFilterData::FilterColumn>& getColumns() const { return _columns; }

    void addColumn(const NLFilterData::FilterColumn& column) {
        _columns.push_back(column);
    }

    ColumnVector<size_t>* getIndices() { return &_indices; }
    std::vector<EdgeRecord>& getRows() { return _rows; }

private:
    const Column* _ids {nullptr};
    NLFetchEdgesFunction _fetch {nullptr};
    bool _undirected {false};
    ColumnNodeIDs* _sources {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _targets {nullptr};
    std::vector<NLFilterData::FilterColumn> _columns;
    ColumnVector<size_t> _indices;

    // Scratch for the rows of this step's edges
    std::vector<EdgeRecord> _rows;
};

}
