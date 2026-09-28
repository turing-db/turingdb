#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "datapart/EdgeRecord.h"
#include "ID.h"
#include "MetadataBuilder.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/SupportedType.h"
#include "properties/PropertyManager.h"
#include "views/GraphView.h"

namespace db {

class ConcurrentWriter;
class EdgeContainer;
class PropertyManager;
class DataPart;
class DataPartMerger;
class CommitBuilder;
class Graph;
class JobSystem;

class DataPartBuilder {
public:
    DataPartBuilder(const DataPartBuilder&) = delete;
    DataPartBuilder(DataPartBuilder&&) = default;
    DataPartBuilder& operator=(const DataPartBuilder&) = delete;
    DataPartBuilder& operator=(DataPartBuilder&&) = default;

    ~DataPartBuilder();

    [[nodiscard]] static std::unique_ptr<DataPartBuilder> prepare(MetadataBuilder& metadata,
                                                                  const GraphView& view,
                                                                  size_t partIndex);

    [[nodiscard]] static std::unique_ptr<DataPartBuilder> prepareMerge(MetadataBuilder& metadata,
                                                                       const GraphView& view);

    NodeID addNode(const LabelSetHandle& labelset);
    NodeID addNode(const LabelSet& labelset);

    template <SupportedType T>
    void addNodeProperty(NodeID nodeID,
                         PropertyTypeID ptID,
                         std::optional<typename T::Primitive>&& value);

    template <SupportedType T>
    void addEdgeProperty(const EdgeRecord& edge,
                         PropertyTypeID ptID,
                         std::optional<typename T::Primitive>&& value,
                         LabelSetHandle srcLblSet = {});

    // A list or map arrives from the write buffer as its owning encoding rather than as a
    // view, which only names elements the buffer that built it still owns.
    template <SupportedType T>
    requires std::same_as<T, types::List> || std::same_as<T, types::Map>
    void addNodeProperty(NodeID nodeID,
                         PropertyTypeID ptID,
                         std::optional<typename T::OwningPrimitive>&& value);

    template <SupportedType T>
    requires std::same_as<T, types::List> || std::same_as<T, types::Map>
    void addEdgeProperty(const EdgeRecord& edge,
                         PropertyTypeID ptID,
                         std::optional<typename T::OwningPrimitive>&& value,
                         LabelSetHandle srcLblSet = {});

    const EdgeRecord& addEdge(EdgeTypeID typeID, NodeID srcID, NodeID tgtID);

    NodeID firstNodeID() const { return _firstNodeID; }
    EdgeID firstEdgeID() const { return _firstEdgeID; }
    size_t nodeCount() const { return _coreNodeLabelSets.size(); }
    size_t edgeCount() const { return _edges.size(); }
    size_t getOutPatchEdgeCount() const { return _outPatchEdgeCount; };
    size_t getInPatchEdgeCount() const { return _inPatchEdgeCount; };
    size_t getPartIndex() const { return _partIndex; };
    std::vector<NodeID>& getTmpNodeIDs() { return _tmpNodeIDVector; }

    // A node or an edge the commit creates and deletes again is built, then tombstoned. It is
    // named here by the ID the builder gave it, which the datapart renumbers as it loads
    void addDeletedNode(NodeID nodeID) { _deletedNodes.push_back(nodeID); }
    void addDeletedEdge(EdgeID edgeID) { _deletedEdges.push_back(edgeID); }
    std::vector<NodeID>& deletedNodes() { return _deletedNodes; }
    std::vector<EdgeID>& deletedEdges() { return _deletedEdges; }

    MetadataBuilder& getMetadata() { return *_metadata; }

    /// Checks if the current property container for T contains a property for I at pid
    template <SupportedType T, TypedInternalID I>
    bool hasProperty(I id, PropertyTypeID pid);

private:
    friend ConcurrentWriter;
    friend DataPart;
    friend DataPartMerger;
    friend CommitBuilder;

    NodeID _firstNodeID {0};
    EdgeID _firstEdgeID {0};
    NodeID _nextNodeID {0};
    EdgeID _nextEdgeID {0};
    MetadataBuilder* _metadata {nullptr};
    GraphView _view;
    size_t _outPatchEdgeCount {0};
    size_t _inPatchEdgeCount {0};
    size_t _partIndex {0};

    std::vector<LabelSetHandle> _coreNodeLabelSets;
    std::vector<NodeID> _tmpNodeIDVector;
    std::vector<NodeID> _deletedNodes;
    std::vector<EdgeID> _deletedEdges;
    std::vector<EdgeRecord> _edges;
    /**
     * @brief Map from EdgeID to EdgeRecord for edges that do not exist in this datapart.
     * @detail This may reference an EdgeRecord in a previous commit, or an EdgeRecord
     * that does not yet exist in any materialised commit. Consider:
     * `CREATE (n:Node)-[e:NEW_EDGE]->(m:Node) SET e.duration = 10`;
     * this query creates two dataparts: first one for the CREATE, secondly one for the
     * SET. This means that 'e' does not exist in any materialised commit when adding
     * e.duration for the SET, as the first datapart is not yet built at that time. This
     * requires the map to own EdgeRecords, as 'e' cannot be a pointer to any materialised
     * EdgeRecord.
     */
    std::unordered_map<EdgeID, EdgeRecord> _patchedEdges;
    std::unordered_set<NodeID> _nodeHasPatchEdges;
    std::unordered_map<NodeID, LabelSetHandle> _patchNodeLabelSets;
    std::unique_ptr<PropertyManager> _nodeProperties;
    std::unique_ptr<PropertyManager> _edgeProperties;

    std::vector<LabelSetHandle>& coreNodeLabelSets() { return _coreNodeLabelSets; }
    std::vector<EdgeRecord>& edges() { return _edges; }
    std::unique_ptr<PropertyManager>& nodeProperties() { return _nodeProperties; }
    std::unique_ptr<PropertyManager>& edgeProperties() { return _edgeProperties; }
    std::unordered_map<NodeID, LabelSetHandle>& patchNodeLabelSets() { return _patchNodeLabelSets; }
    std::unordered_map<EdgeID, EdgeRecord>& patchedEdges() { return _patchedEdges; }
    size_t patchNodeEdgeDataCount() const {
        return _nodeHasPatchEdges.size();
    }

    [[nodiscard]] static std::unique_ptr<DataPartBuilder> create(MetadataBuilder& metadata,
                                                                 const GraphView& view,
                                                                 size_t nodeCount,
                                                                 size_t edgeCount,
                                                                 size_t partIndex);

    size_t getNodeEmbeddingDimension(PropertyTypeID ptID) const;
    size_t getEdgeEmbeddingDimension(PropertyTypeID ptID) const;

    DataPartBuilder() = default;
};

}
