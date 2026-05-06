#pragma once

#include <unordered_map>
#include <vector>

#include "DumpResult.h"
#include "GraphLoaderInterface.h"
#include "Path.h"
#include "versioning/CommitHash.h"
#include "versioning/DataPartID.h"
#include "ArcManager.h"

namespace db {

class Graph;
class Commit;
class CommitData;
class DataPart;
class GraphMetadata;
class CommitJournal;
class Tombstones;
class PropertyManager;
class StringPropertyIndexer;
class StringIndex;
class PropertyContainer;
class NodeContainer;
class EdgeContainer;
class EdgeIndexer;
class CommitHistoryBuilder;

/**
 * @brief Concrete @ref GraphLoaderInterface implementation that builds the in-memory
 * Graph/Commit/DataPart object tree from primitive values streamed by a @ref DiskDecoder.
 * This is the only class that touches storage-layer private state during loading.
 *
 * Also exposes the public static @ref load entry point used by callers (matches the
 * historical signature so callers don't change).
 */
class GraphLoader : public GraphLoaderInterface {
public:
    explicit GraphLoader(Graph* graph);
    ~GraphLoader() override;

    GraphLoader(const GraphLoader&) = delete;
    GraphLoader(GraphLoader&&) = delete;
    GraphLoader& operator=(const GraphLoader&) = delete;
    GraphLoader& operator=(GraphLoader&&) = delete;

    [[nodiscard]] static DumpResult<void> load(Graph* graph, const fs::Path& graphDir);

    // ── GraphLoaderInterface ────────────────────────────────
    void setGraphInfo(uint64_t graphID, std::string_view name) override;
    void initVersionController() override;

    void reserveCommits(uint64_t numCommits) override;
    void addCommitSkeleton(uint64_t commitHash,
                           uint64_t numNodes,
                           uint64_t numEdges,
                           uint64_t numCommitDataParts) override;
    uint64_t getHeadCommitHash() const override;
    void setNumCommits(uint64_t n) override;

    void beginCommitData(uint64_t commitHash) override;
    void endCommitData() override;

    uint64_t addLabel(std::string_view name) override;
    uint64_t addEdgeType(std::string_view name) override;
    uint64_t addPropertyType(std::string_view name, ValueType vt) override;
    uint64_t addLabelSet(const uint64_t* bits, size_t count) override;

    void addNodeWriteSet(const uint64_t* nodeIDs, size_t count) override;
    void addEdgeWriteSet(const uint64_t* edgeIDs, size_t count) override;

    void addNodeTombstones(const uint64_t* nodeIDs, size_t count) override;
    void addEdgeTombstones(const uint64_t* edgeIDs, size_t count) override;

    bool isDataPartLoaded(uint64_t partID) const override;
    void attachExistingDataPart(uint64_t partID) override;

    void beginDataPart(uint64_t partID,
                       uint64_t firstNodeID,
                       uint64_t firstEdgeID) override;
    void endDataPart() override;
    void attachDataPartToCommit(uint64_t partID) override;
    void setCommitDataPartCount(uint64_t count) override;

    void beginNodeContainer(uint64_t firstNodeID, uint64_t nodeCount) override;
    void addNodeRange(uint64_t labelsetID, uint64_t firstNodeID, uint64_t count) override;
    void appendNodeRecords(const uint64_t* labelsetIDs, size_t count) override;
    void endNodeContainer() override;
    void initEmptyNodeContainer() override;

    void beginEdgeContainer(uint64_t firstNodeID,
                            uint64_t firstEdgeID,
                            uint64_t edgeCount) override;
    void appendEdgeRecords(EdgeDirection dir,
                           const EdgeRecordRaw* records,
                           size_t count) override;
    void endEdgeContainer() override;
    void initEmptyEdgeContainer() override;

    void beginEdgeIndexer(uint64_t firstNodeID,
                          uint64_t firstEdgeID,
                          uint64_t coreNodeCount,
                          uint64_t patchNodeCount) override;
    void appendNodeEdgeRanges(const NodeEdgeRangesRaw* ranges, size_t count) override;
    void finalizeEdgeIndexerPatchNodes() override;
    void appendEdgeIndexerSpan(EdgeDirection dir,
                               uint64_t labelsetID,
                               uint64_t offset,
                               uint64_t count) override;
    void endEdgeIndexer() override;

    void appendPropertyIndexerEntry(EntityKind kind,
                                    uint64_t propTypeID,
                                    uint64_t labelsetID,
                                    const PropIndexRangeRaw* ranges,
                                    size_t count) override;

    void registerPropertyContainer(EntityKind kind,
                                   uint64_t propTypeID,
                                   ValueType vt,
                                   uint64_t propCount,
                                   uint64_t embeddingDimension) override;
    void appendPropertyIDs(EntityKind kind,
                           uint64_t propTypeID,
                           const uint64_t* entityIDs,
                           size_t count) override;
    void appendUInt64PropertyValues(EntityKind kind,
                                    uint64_t propTypeID,
                                    const uint64_t* values,
                                    size_t count) override;
    void appendInt64PropertyValues(EntityKind kind,
                                   uint64_t propTypeID,
                                   const int64_t* values,
                                   size_t count) override;
    void appendDoublePropertyValues(EntityKind kind,
                                    uint64_t propTypeID,
                                    const double* values,
                                    size_t count) override;
    void appendBoolPropertyValues(EntityKind kind,
                                  uint64_t propTypeID,
                                  const uint8_t* values,
                                  size_t count) override;
    void appendStringPropertyValue(EntityKind kind,
                                   uint64_t propTypeID,
                                   std::string_view value) override;
    void appendEmbeddingPropertyValues(EntityKind kind,
                                       uint64_t propTypeID,
                                       const float* floats,
                                       size_t numEmbeddings,
                                       size_t dimension) override;
    void finalizePropertyContainer(EntityKind kind, uint64_t propTypeID) override;

    void beginStringPropIndex(EntityKind kind,
                              uint64_t propTypeID,
                              uint64_t numTreeNodes) override;
    void setStringPropIndexChild(EntityKind kind,
                                 uint64_t propTypeID,
                                 uint64_t parentLocalID,
                                 uint64_t childSlot,
                                 uint64_t childLocalID) override;
    void appendStringPropIndexOwners(EntityKind kind,
                                     uint64_t propTypeID,
                                     uint64_t treeNodeLocalID,
                                     const uint64_t* ownerEntityIDs,
                                     size_t count) override;
    void endStringPropIndex(EntityKind kind, uint64_t propTypeID) override;
    void finalizeStringPropIndexer(EntityKind kind) override;

    void finalizeDataPart() override;

    // ── Helpers used by CommitLoader::loadData façade ───────
    Commit* findCommit(uint64_t commitHash) const;

private:
    PropertyManager& currentPropManager(EntityKind kind);
    StringPropertyIndexer& currentStringIndexer(EntityKind kind);
    PropertyContainer& currentPropertyContainer(EntityKind kind, uint64_t propTypeID);
    StringIndex& currentStringIndex(EntityKind kind, uint64_t propTypeID);

    Graph* _graph {nullptr};
    Commit* _commit {nullptr};
    CommitData* _commitData {nullptr};
    GraphMetadata* _metadata {nullptr};
    CommitJournal* _journal {nullptr};
    Tombstones* _tombstones {nullptr};
    std::unique_ptr<CommitHistoryBuilder> _historyBuilder;

    WeakArc<DataPart> _currentPart;
    DataPart* _partRaw {nullptr};

    // Last accessed datapart by ID (used by partMap dedup attachExistingDataPart).
    std::unordered_map<DataPartID, WeakArc<DataPart>>* _partMap {nullptr};

    const Commit* _prevCommit {nullptr};

    // Per-datapart streaming write offsets, reset in begin* methods.
    size_t _nodeRecordOffset {0};
    size_t _outEdgeOffset {0};
    size_t _inEdgeOffset {0};
    size_t _edgeIndexerNodeOffset {0};
    size_t _edgeIndexerPatchCount {0};
};

}
