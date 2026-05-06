#pragma once

#include <stddef.h>
#include <stdint.h>
#include <span>
#include <string_view>

#include "metadata/PropertyType.h"

namespace db {

// POD payload structs — designed to cross a WASM boundary as ptr+count.
struct EdgeRecordRaw {
    uint64_t _edgeID;
    uint64_t _nodeID;
    uint64_t _otherID;
    uint64_t _edgeTypeID;
};

struct NodeEdgeRangesRaw {
    uint64_t _outFirst;
    uint64_t _outCount;
    uint64_t _inFirst;
    uint64_t _inCount;
};

struct PropIndexRangeRaw {
    uint64_t _offset;
    uint64_t _count;
};

enum class EntityKind : uint8_t {
    Node = 0,
    Edge = 1,
};

enum class EdgeDirection : uint8_t {
    Out = 0,
    In = 1,
};

class GraphLoaderInterface {
public:
    virtual ~GraphLoaderInterface() = default;

    // ─────────────────────────────────────────────
    // Graph
    // ─────────────────────────────────────────────
    virtual void setGraphInfo(uint64_t graphID, std::string_view name) = 0;
    virtual void initVersionController() = 0;

    virtual void reserveCommits(uint64_t numCommits) = 0;
    virtual void addCommitSkeleton(uint64_t commitHash,
                                   uint64_t numNodes,
                                   uint64_t numEdges,
                                   uint64_t numCommitDataParts) = 0;
    virtual uint64_t getHeadCommitHash() const = 0;
    virtual void setNumCommits(uint64_t n) = 0;

    // ─────────────────────────────────────────────
    // Per-commit data lifecycle
    // ─────────────────────────────────────────────
    virtual void beginCommitData(uint64_t commitHash) = 0;
    virtual void endCommitData() = 0;

    // Graph metadata maps (within the active commit)
    virtual uint64_t addLabel(std::string_view name) = 0;
    virtual uint64_t addEdgeType(std::string_view name) = 0;
    virtual uint64_t addPropertyType(std::string_view name, ValueType vt) = 0;
    virtual uint64_t addLabelSet(const uint64_t* bits, size_t count) = 0;

    // Commit journal (within the active commit)
    virtual void addNodeWriteSet(const uint64_t* nodeIDs, size_t count) = 0;
    virtual void addEdgeWriteSet(const uint64_t* edgeIDs, size_t count) = 0;

    // Tombstones (within the active commit)
    virtual void addNodeTombstones(const uint64_t* nodeIDs, size_t count) = 0;
    virtual void addEdgeTombstones(const uint64_t* edgeIDs, size_t count) = 0;

    // ─────────────────────────────────────────────
    // DataParts
    // ─────────────────────────────────────────────
    virtual bool isDataPartLoaded(uint64_t partID) const = 0;

    /// Reuse an already-loaded datapart for the active commit.
    virtual void attachExistingDataPart(uint64_t partID) = 0;

    virtual void beginDataPart(uint64_t partID,
                               uint64_t firstNodeID,
                               uint64_t firstEdgeID) = 0;
    virtual void endDataPart() = 0;

    /// Notify that the just-built / attached datapart belongs to the active commit.
    virtual void attachDataPartToCommit(uint64_t partID) = 0;

    /// Sets how many of the trailing dataparts in commit history are "this commit's".
    virtual void setCommitDataPartCount(uint64_t count) = 0;

    // ── Within active datapart: NodeContainer
    virtual void beginNodeContainer(uint64_t firstNodeID, uint64_t nodeCount) = 0;
    virtual void addNodeRange(uint64_t labelsetID,
                              uint64_t firstNodeID,
                              uint64_t count) = 0;
    virtual void appendNodeRecords(const uint64_t* labelsetIDs, size_t count) = 0;
    virtual void endNodeContainer() = 0;
    virtual void initEmptyNodeContainer() = 0;

    // ── Within active datapart: EdgeContainer
    virtual void beginEdgeContainer(uint64_t firstNodeID,
                                    uint64_t firstEdgeID,
                                    uint64_t edgeCount) = 0;
    virtual void appendEdgeRecords(EdgeDirection dir,
                                   const EdgeRecordRaw* records,
                                   size_t count) = 0;
    virtual void endEdgeContainer() = 0;
    virtual void initEmptyEdgeContainer() = 0;

    // ── Within active datapart: EdgeIndexer
    virtual void beginEdgeIndexer(uint64_t firstNodeID,
                                  uint64_t firstEdgeID,
                                  uint64_t coreNodeCount,
                                  uint64_t patchNodeCount) = 0;
    virtual void appendNodeEdgeRanges(const NodeEdgeRangesRaw* ranges,
                                      size_t count) = 0;
    /// Build the patchNodeOffsets index — call once node-edge ranges are populated,
    /// before appending labelset spans.
    virtual void finalizeEdgeIndexerPatchNodes() = 0;
    virtual void appendEdgeIndexerSpan(EdgeDirection dir,
                                       uint64_t labelsetID,
                                       uint64_t offset,
                                       uint64_t count) = 0;
    virtual void endEdgeIndexer() = 0;

    // ── Within active datapart: Property indexer
    virtual void appendPropertyIndexerEntry(EntityKind kind,
                                            uint64_t propTypeID,
                                            uint64_t labelsetID,
                                            const PropIndexRangeRaw* ranges,
                                            size_t count) = 0;

    // ── Within active datapart: Property containers
    virtual void registerPropertyContainer(EntityKind kind,
                                           uint64_t propTypeID,
                                           ValueType vt,
                                           uint64_t propCount,
                                           uint64_t embeddingDimension /* 0 if not embedding */) = 0;
    virtual void appendPropertyIDs(EntityKind kind,
                                   uint64_t propTypeID,
                                   const uint64_t* entityIDs,
                                   size_t count) = 0;
    virtual void appendUInt64PropertyValues(EntityKind kind,
                                            uint64_t propTypeID,
                                            const uint64_t* values,
                                            size_t count) = 0;
    virtual void appendInt64PropertyValues(EntityKind kind,
                                           uint64_t propTypeID,
                                           const int64_t* values,
                                           size_t count) = 0;
    virtual void appendDoublePropertyValues(EntityKind kind,
                                            uint64_t propTypeID,
                                            const double* values,
                                            size_t count) = 0;
    virtual void appendBoolPropertyValues(EntityKind kind,
                                          uint64_t propTypeID,
                                          const uint8_t* values,
                                          size_t count) = 0;
    virtual void appendStringPropertyValue(EntityKind kind,
                                           uint64_t propTypeID,
                                           std::string_view value) = 0;
    virtual void appendEmbeddingPropertyValues(EntityKind kind,
                                               uint64_t propTypeID,
                                               const float* floats,
                                               size_t numEmbeddings,
                                               size_t dimension) = 0;
    virtual void finalizePropertyContainer(EntityKind kind, uint64_t propTypeID) = 0;

    // ── Within active datapart: String property indexer (per-ptID prefix tree)
    virtual void beginStringPropIndex(EntityKind kind,
                                      uint64_t propTypeID,
                                      uint64_t numTreeNodes) = 0;
    virtual void setStringPropIndexChild(EntityKind kind,
                                         uint64_t propTypeID,
                                         uint64_t parentLocalID,
                                         uint64_t childSlot,
                                         uint64_t childLocalID) = 0;
    virtual void appendStringPropIndexOwners(EntityKind kind,
                                             uint64_t propTypeID,
                                             uint64_t treeNodeLocalID,
                                             const uint64_t* ownerEntityIDs,
                                             size_t count) = 0;
    virtual void endStringPropIndex(EntityKind kind, uint64_t propTypeID) = 0;
    virtual void finalizeStringPropIndexer(EntityKind kind) = 0;

    virtual void finalizeDataPart() = 0;
};

}
