#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ID.h"
#include "LocalMemory.h"
#include "NLGroupHashTable.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

#include "NLWriteProperties.h"
#include "NLFunctionData.h"
#include "NLColumnFunctions.h"

#include "StringHashMap.h"

namespace db {

class NLRuntimeProperty;
struct NLMergeWorkingSet;

// One property a CREATE writes and the column its values come from. A column of tagged
// cells carries the runtime property that types it as the write runs in place of a type ID.
struct NLCreatedProperty {
    PropertyTypeID _propertyTypeID;
    const Column* _values {nullptr};
    NLRuntimeProperty* _runtimeProperty {nullptr};
};

class NLCreateNodeData : public NLFunctionData {
public:
    using Property = NLCreatedProperty;

    NLCreateNodeData(LabelSetHandle labelsetHandle, ColumnNodeIDs* result)
        : _result(result),
        _labelsetHandle(labelsetHandle)
    {
    }

    LabelSetHandle getLabelSetHandle() const { return _labelsetHandle; }
    ColumnNodeIDs* getResult() const { return _result; }
    const std::vector<Property>& properties() const { return _properties; }

    size_t getRowCount() const { return _cardinality ? _cardinality->size() : 1; }

    void addProperty(const Property& property) {
        _properties.push_back(property);
    }

    void setCardinality(const Column* cardinality) {
        _cardinality = cardinality;
    }

private:
    std::vector<Property> _properties;
    ColumnNodeIDs* _result {nullptr};
    const Column* _cardinality {nullptr};
    LabelSetHandle _labelsetHandle;
};

class NLCreateEdgeData : public NLFunctionData {
public:
    using Property = NLCreatedProperty;

    NLCreateEdgeData(EdgeTypeID edgeTypeID,
                     const ColumnNodeIDs* src,
                     bool srcIsPending,
                     const ColumnNodeIDs* tgt,
                     bool tgtIsPending,
                     ColumnEdgeIDs* result)
        : _edgeTypeID(edgeTypeID),
        _src(src),
        _tgt(tgt),
        _result(result),
        _srcIsPending(srcIsPending),
        _tgtIsPending(tgtIsPending)
    {
    }

    EdgeTypeID getEdgeTypeID() const { return _edgeTypeID; }

    const ColumnNodeIDs* getSrc() const { return _src; }

    bool isSrcPending() const { return _srcIsPending; }

    const ColumnNodeIDs* getTgt() const { return _tgt; }

    bool isTgtPending() const { return _tgtIsPending; }

    // Row by row, which endpoints are pending, for a column a merge produced and so
    // mixed. Null for a column that is pending in every row or in none, which the two
    // flags above then answer for.
    const ColumnVector<CustomBool>* getSrcPendingMask() const { return _srcPendingMask; }
    const ColumnVector<CustomBool>* getTgtPendingMask() const { return _tgtPendingMask; }

    void setSrcPendingMask(const ColumnVector<CustomBool>* mask) { _srcPendingMask = mask; }
    void setTgtPendingMask(const ColumnVector<CustomBool>* mask) { _tgtPendingMask = mask; }

    ColumnEdgeIDs* getResult() const { return _result; }

    const std::vector<Property>& properties() const { return _properties; }

    void addProperty(const Property& property) {
        _properties.push_back(property);
    }

private:
    std::vector<Property> _properties;
    EdgeTypeID _edgeTypeID;
    const ColumnNodeIDs* _src {nullptr};
    const ColumnNodeIDs* _tgt {nullptr};
    const ColumnVector<CustomBool>* _srcPendingMask {nullptr};
    const ColumnVector<CustomBool>* _tgtPendingMask {nullptr};
    ColumnEdgeIDs* _result {nullptr};
    bool _srcIsPending {false};
    bool _tgtIsPending {false};
};

// Which way a merge pattern's hop points, from the chain node ahead of it to the one
// behind. The interpreter's copy of the storage-dialect EdgeDirection, set during
// translation so execution never reaches into MLIR.
enum class NLMergeDirection {
    Undirected = 0,
    Backward,
    Forward,
};

// A node or edge one row of a merge bound: an ID the graph holds, or - when pending -
// an offset into the change's write buffer for an entity this query wrote and has not
// committed. The two spaces overlap numerically, so the flag is what tells them apart.
struct NLMergeRef {
    uint64_t _id {0};
    bool _pending {false};

    bool operator==(const NLMergeRef& other) const {
        return _id == other._id && _pending == other._pending;
    }

    // The two spaces packed into one integer, so a ref can key a map or a set
    uint64_t asKey() const { return (_id << 1) | static_cast<uint64_t>(_pending); }
};

// One property constraint a merge pattern's node or hop puts on a row: the property it
// names and the column the row's asked-for value comes from, with the appender that
// turns that value into its part of the key the match looks up.
struct NLMergeProperty {
    PropertyType _propertyType;
    const Column* _values {nullptr};
    NLKeyAppendFunction _keyAppend {nullptr};

    // Set for a column of tagged cells, whose rows are keyed as the values they are staged
    // as. The type stays unset until the merge's first chunk holding a value types it.
    NLRuntimeProperty* _runtimeProperty {nullptr};
};

// The graph side of that key: the same property, read out of the graph into a scratch
// column. Its appender keys a fetched (and so nullable) value the way the row's own
// appender keys the asked-for one, which is what lets the two keys be compared.
struct NLMergeScanProperty {
    PropertyType _propertyType;
    Column* _values {nullptr};
    NLKeyAppendFunction _keyAppend {nullptr};
};

using NLMergeScanProperties = std::vector<NLMergeScanProperty>;

// The nodes the graph holds under one chain-node spec, indexed by the spec's property
// values. Scanned once, on first use, so a merge driven by many rows pays for one pass
// over its label set rather than a lookup per row. The nodes this query writes, whichever
// clause writes them, are taken in as they are written, keyed by the same values read
// back from the write buffer.
class NLMergeNodeIndex {
public:
    NLMergeNodeIndex(const LabelSet& labels,
                     const LabelSet& writeLabels,
                     bool matchable,
                     ColumnNodeIDs* scanNodes);
    ~NLMergeNodeIndex();

    const LabelSet& getLabels() const { return _labels; }
    const LabelSet& getWriteLabels() const { return _writeLabels; }

    // False when a label or a key property of the spec is absent from the graph's
    // schema: no committed node can carry it, so the graph offers no candidate and only
    // the pending nodes this change wrote can match
    bool isMatchable() const { return _matchable; }

    bool isBuilt() const { return _built; }
    void markBuilt() { _built = true; }

    ColumnNodeIDs* getScanNodes() const { return _scanNodes; }

    const std::vector<NLMergeScanProperty>& scanProperties() const { return _scanProperties; }
    void addScanProperty(const NLMergeScanProperty& property) { _scanProperties.push_back(property); }

    // The spec's key properties in the types they are written with, which is what a
    // pending node's values are read back through
    const NLMergeScanProperties& writtenProperties() const { return _writtenProperties; }
    NLMergeScanProperties& writtenProperties() { return _writtenProperties; }
    void addWrittenProperty(const NLMergeScanProperty& property) { _writtenProperties.push_back(property); }

    size_t getNextPendingNode() const { return _nextPendingNode; }
    void setNextPendingNode(size_t offset) { _nextPendingNode = offset; }

    // How far the index has read the updates to the graph's nodes and to the ones this
    // query wrote. A node an update reached after the index took it in is keyed again
    // under what it holds now, and its entries under earlier values stay behind: a lookup
    // checks such a node against its current values.
    size_t getNextNodeUpdate() const { return _nextNodeUpdate; }
    void setNextNodeUpdate(size_t position) { _nextNodeUpdate = position; }

    size_t getNextPendingNodeUpdate() const { return _nextPendingNodeUpdate; }
    void setNextPendingNodeUpdate(size_t position) { _nextPendingNodeUpdate = position; }

    bool hasChanged(const NLMergeRef& ref) const { return _changedNodes.contains(ref.asKey()); }
    void markChanged(const NLMergeRef& ref) { _changedNodes.insert(ref.asKey()); }

    void add(std::string_view key, const NLMergeRef& ref) { _byKey[std::string(key)].push_back(ref); }

    std::span<const NLMergeRef> find(std::string_view key) const;

private:
    LabelSet _labels;
    LabelSet _writeLabels;
    std::vector<NLMergeScanProperty> _scanProperties;
    NLMergeScanProperties _writtenProperties;
    StringHashMap<std::string, std::vector<NLMergeRef>> _byKey;
    std::unordered_set<uint64_t> _changedNodes;
    ColumnNodeIDs* _scanNodes {nullptr};
    size_t _nextPendingNode {0};
    size_t _nextNodeUpdate {0};
    size_t _nextPendingNodeUpdate {0};
    bool _matchable {false};
    bool _built {false};
};

// Every edge this change wrote, whichever clause or statement wrote it, under each of the
// two nodes it joins: a pending edge is in no graph the match reads, so this is where a
// hop finds it. One log per program, shared by every merge op in it, and taken in from the
// write buffer as the query writes.
class NLMergePendingEdges {
public:
    struct Entry {
        NLMergeRef _other;
        EdgeTypeID _edgeType;
        uint64_t _offset {0};
    };

    NLMergePendingEdges();
    ~NLMergePendingEdges();

    void add(const NLMergeRef& source, const NLMergeRef& target, EdgeTypeID edgeType, uint64_t offset);

    size_t getNextPendingEdge() const { return _nextPendingEdge; }
    void setNextPendingEdge(size_t offset) { _nextPendingEdge = offset; }

    std::span<const Entry> outOf(const NLMergeRef& node) const;
    std::span<const Entry> into(const NLMergeRef& node) const;

private:
    std::unordered_map<uint64_t, std::vector<Entry>> _outgoing;
    std::unordered_map<uint64_t, std::vector<Entry>> _incoming;
    size_t _nextPendingEdge {0};

    static std::span<const Entry> lookup(const std::unordered_map<uint64_t, std::vector<Entry>>& edges,
                                         const NLMergeRef& node);
};

// nl.merge data: one entry per chain node and one per hop of the pattern, the columns
// each row's values and bound entities come from, and the chunks the op fills.
class NLMergeData : public NLFunctionData {
public:
    // One node of the chain. A bound node reads its rows from _boundColumn and is never
    // written; every other one is looked up through _index and, when the pattern is
    // missing, written under _labelSetHandle with _properties' values. Only a looked-up
    // node holds output chunks: a bound one's rows come back through the carry set.
    struct Node {
        std::vector<NLMergeProperty> _properties;

        const ColumnNodeIDs* _boundColumn {nullptr};
        const ColumnVector<CustomBool>* _boundPending {nullptr};
        NLMergeNodeIndex* _index {nullptr};
        LabelSetHandle _labelSetHandle;
        ColumnNodeIDs* _output {nullptr};
        ColumnVector<CustomBool>* _outputPending {nullptr};
    };

    // One hop of the chain, joining the node ahead of it to the one behind. The match
    // edge type is invalid when the graph's schema does not have it, which leaves only
    // the pending edges to match. The scratch chunks below are what a hop constraining
    // properties reads its candidates' values through: the graph's in the types the graph
    // holds them as, the pending ones' in the types they are written with.
    struct Hop {
        std::vector<NLMergeProperty> _properties;
        std::vector<NLMergeScanProperty> _scanProperties;
        NLMergeScanProperties _writtenProperties;

        EdgeTypeID _matchEdgeType;
        EdgeTypeID _writeEdgeType;
        NLMergeDirection _direction {NLMergeDirection::Forward};
        ColumnNodeIDs* _scanSources {nullptr};
        ColumnEdgeIDs* _scanEdges {nullptr};
        ColumnEdgeIDs* _output {nullptr};
        ColumnVector<CustomBool>* _outputPending {nullptr};
    };

    NLMergeData(NLMergePendingEdges* pendingEdges, ColumnVector<CustomBool>* created, LocalMemory* memory);
    ~NLMergeData() override;

    const std::vector<Node>& nodes() const { return _nodes; }
    const std::vector<Hop>& hops() const { return _hops; }
    const std::vector<NLCarriedColumn>& carriedColumns() const { return _carriedColumns; }

    std::vector<Node>& nodes() { return _nodes; }
    std::vector<Hop>& hops() { return _hops; }

    NLMergePendingEdges* getPendingEdges() const { return _pendingEdges; }
    ColumnVector<CustomBool>* getCreated() const { return _created; }

    LocalMemory* getMemory() const { return _memory; }

    // The column whose length is the number of rows the op runs over: a chunk some part
    // of the pattern reads. Null for a pattern reading none, which is the single row a
    // MERGE standing on its own writes.
    const Column* getRowCarrier() const { return _rowCarrier; }
    void setRowCarrier(const Column* rowCarrier) { _rowCarrier = rowCarrier; }

    void addNode(const Node& node) { _nodes.push_back(node); }
    void addHop(const Hop& hop) { _hops.push_back(hop); }
    void addCarriedColumn(const NLCarriedColumn& carried) { _carriedColumns.push_back(carried); }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    // The scratch every step of the op works in, allocated once with the op rather than
    // once per chunk
    NLMergeWorkingSet* getWorkingSet() const { return _workingSet.get(); }

private:
    std::vector<Node> _nodes;
    std::vector<Hop> _hops;
    std::vector<NLCarriedColumn> _carriedColumns;
    NLMergePendingEdges* _pendingEdges {nullptr};
    ColumnVector<CustomBool>* _created {nullptr};
    LocalMemory* _memory {nullptr};
    const Column* _rowCarrier {nullptr};
    std::unique_ptr<NLMergeWorkingSet> _workingSet;

    // This step's row map, from each emitted row to the input row behind it, fed to the
    // per-column gather that rebuilds the carry set
    ColumnVector<size_t> _indices;
};

class NLSetNodePropertyData : public NLFunctionData {
public:
    NLSetNodePropertyData(PropertyTypeID propertyTypeID,
                          const ColumnNodeIDs* input,
                          const Column* value)
        : _input(input),
        _value(value),
        _propertyTypeID(propertyTypeID)
    {
    }

    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }
    const ColumnNodeIDs* getInput() const { return _input; }
    const Column* getValue() const { return _value; }

    // The rows whose node this change wrote and has not committed: those are updated in
    // the write buffer's pending node rather than recorded as an update to a committed
    // one. Null for an input no merge produced.
    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    // The rows the write touches, or null for a write that touches every one: Cypher's
    // ON CREATE hands over the merge's created mask and ON MATCH its negation.
    const ColumnVector<CustomBool>* getRows() const { return _rows; }
    void setRows(const ColumnVector<CustomBool>* rows) { _rows = rows; }

    // A null or a column of tagged cells reaches a property the program types as it runs:
    // a removal stages a null of the type it has by then, and every cell is checked against
    // that type and staged as it. Null for a column of a type the write was translated with.
    NLRuntimeProperty* getRuntimeProperty() const { return _runtimeProperty; }
    void setRuntimeProperty(NLRuntimeProperty* runtimeProperty) { _runtimeProperty = runtimeProperty; }

    bool writesNull() const { return _writesNull; }
    void setWritesNull(bool writesNull) { _writesNull = writesNull; }

    // The rows of this step the write reaches, which alone type and check its cells
    ColumnVector<CustomBool>* getWrittenRows() const { return _writtenRows; }
    void setWrittenRows(ColumnVector<CustomBool>* writtenRows) { _writtenRows = writtenRows; }

private:
    const ColumnNodeIDs* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    const ColumnVector<CustomBool>* _rows {nullptr};
    NLRuntimeProperty* _runtimeProperty {nullptr};
    ColumnVector<CustomBool>* _writtenRows {nullptr};
    PropertyTypeID _propertyTypeID;
    bool _writesNull {false};
    bool _allPending {false};
};

class NLSetEdgePropertyData : public NLFunctionData {
public:
    NLSetEdgePropertyData(PropertyTypeID propertyTypeID,
                          const ColumnEdgeIDs* input,
                          const Column* value)
        : _input(input),
        _value(value),
        _propertyTypeID(propertyTypeID)
    {
    }

    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }
    const ColumnEdgeIDs* getInput() const { return _input; }
    const Column* getValue() const { return _value; }

    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    const ColumnVector<CustomBool>* getRows() const { return _rows; }
    void setRows(const ColumnVector<CustomBool>* rows) { _rows = rows; }

    // A null or a column of tagged cells reaches a property the program types as it runs:
    // a removal stages a null of the type it has by then, and every cell is checked against
    // that type and staged as it. Null for a column of a type the write was translated with.
    NLRuntimeProperty* getRuntimeProperty() const { return _runtimeProperty; }
    void setRuntimeProperty(NLRuntimeProperty* runtimeProperty) { _runtimeProperty = runtimeProperty; }

    bool writesNull() const { return _writesNull; }
    void setWritesNull(bool writesNull) { _writesNull = writesNull; }

    // The rows of this step the write reaches, which alone type and check its cells
    ColumnVector<CustomBool>* getWrittenRows() const { return _writtenRows; }
    void setWrittenRows(ColumnVector<CustomBool>* writtenRows) { _writtenRows = writtenRows; }

private:
    const ColumnEdgeIDs* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    const ColumnVector<CustomBool>* _rows {nullptr};
    NLRuntimeProperty* _runtimeProperty {nullptr};
    ColumnVector<CustomBool>* _writtenRows {nullptr};
    PropertyTypeID _propertyTypeID;
    bool _writesNull {false};
    bool _allPending {false};
};

template <typename IDColumn>
class NLSetPropertiesData : public NLFunctionData {
public:
    using IDColumnType = IDColumn;

    NLSetPropertiesData(const IDColumn* input, const Column* value, bool replaces)
        : _input(input),
        _value(value),
        _replaces(replaces)
    {
    }

    const IDColumn* getInput() const { return _input; }
    const Column* getValue() const { return _value; }
    bool getReplaces() const { return _replaces; }

    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    const ColumnVector<CustomBool>* getRows() const { return _rows; }
    void setRows(const ColumnVector<CustomBool>* rows) { _rows = rows; }

    bool writesNull() const { return _writesNull; }
    void setWritesNull(bool writesNull) { _writesNull = writesNull; }

    std::vector<WrittenMap>& getMaps() { return _maps; }
    CommitWriteBuffer::UntypedProperties& getMapProperties() { return _mapProperties; }
    std::vector<StagedRange>& getMapRanges() { return _mapRanges; }
    CommitWriteBuffer::UntypedProperties& getRemovals() { return _removals; }
    std::vector<PropertyTypeID>& getHeld() { return _held; }
    std::vector<ValueType>& getHeldTypes() { return _heldTypes; }
    std::vector<MapKeyPropertyType>& getKeyTypes() { return _keyTypes; }

private:
    const IDColumn* _input {nullptr};
    const Column* _value {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    const ColumnVector<CustomBool>* _rows {nullptr};
    bool _allPending {false};
    bool _replaces {false};
    bool _writesNull {false};
    std::vector<WrittenMap> _maps;
    CommitWriteBuffer::UntypedProperties _mapProperties;
    std::vector<StagedRange> _mapRanges;
    CommitWriteBuffer::UntypedProperties _removals;
    std::vector<PropertyTypeID> _held;
    std::vector<ValueType> _heldTypes;
    std::vector<MapKeyPropertyType> _keyTypes;
};

using NLSetNodePropertiesData = NLSetPropertiesData<ColumnNodeIDs>;
using NLSetEdgePropertiesData = NLSetPropertiesData<ColumnEdgeIDs>;

class NLDeleteNodeData : public NLFunctionData {
public:
    NLDeleteNodeData(const ColumnNodeIDs* input, bool detaching, ColumnNodeIDs* committed)
        : _input(input),
        _committed(committed),
        _detaching(detaching)
    {
    }

    const ColumnNodeIDs* getInput() const { return _input; }
    bool isDetaching() const { return _detaching; }

    // The rows naming a node this change wrote and has not committed - a write-buffer
    // offset rather than an ID the graph holds - or null when no row does. A column a
    // create produced is such a row throughout, which is what _allPending says.
    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    // The scratch the committed rows of a mixed column are gathered into, since the
    // deletion of those goes through the graph's own IDs
    ColumnNodeIDs* getCommitted() const { return _committed; }

private:
    const ColumnNodeIDs* _input {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    ColumnNodeIDs* _committed {nullptr};
    bool _detaching {false};
    bool _allPending {false};
};

class NLDeleteEdgeData : public NLFunctionData {
public:
    NLDeleteEdgeData(const ColumnEdgeIDs* input, ColumnEdgeIDs* committed)
        : _input(input),
        _committed(committed)
    {
    }

    // The edge siblings of NLDeleteNodeData's
    const ColumnVector<CustomBool>* getPending() const { return _pending; }
    void setPending(const ColumnVector<CustomBool>* pending) { _pending = pending; }

    bool isAllPending() const { return _allPending; }
    void setAllPending(bool allPending) { _allPending = allPending; }

    ColumnEdgeIDs* getCommitted() const { return _committed; }

    const ColumnEdgeIDs* getInput() const { return _input; }

private:
    const ColumnEdgeIDs* _input {nullptr};
    const ColumnVector<CustomBool>* _pending {nullptr};
    ColumnEdgeIDs* _committed {nullptr};
    bool _allPending {false};
};

}
