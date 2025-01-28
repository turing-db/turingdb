#pragma once

#include <memory>
#include <unordered_set>

#include "EdgeRecord.h"
#include "EntityID.h"
#include "properties/PropertyManager.h"

namespace db {

class ConcurrentWriter;
class EdgeContainer;
class PropertyManager;
class DataPart;
class Graph;
class JobSystem;

class DataPartBuilder {
public:
    DataPartBuilder(EntityID firstNodeID,
                    EntityID firstEdgeID,
                    Graph* graph);

    DataPartBuilder(const DataPartBuilder&) = delete;
    DataPartBuilder(DataPartBuilder&&) = default;
    DataPartBuilder& operator=(const DataPartBuilder&) = delete;
    DataPartBuilder& operator=(DataPartBuilder&&) = default;

    ~DataPartBuilder();

    EntityID addNode(const LabelSetID& labelset);
    EntityID addNode(const LabelSet& labelset);

    template <SupportedType T>
    void addNodeProperty(EntityID nodeID,
                         PropertyTypeID ptID,
                         T::Primitive value);

    template <SupportedType T>
    void addEdgeProperty(const EdgeRecord& edge,
                         PropertyTypeID ptID,
                         T::Primitive value);

    const EdgeRecord& addEdge(EdgeTypeID typeID, EntityID srcID, EntityID tgtID);

    EntityID firstNodeID() const { return _firstNodeID; }
    EntityID firstEdgeID() const { return _firstEdgeID; }
    size_t nodeCount() const { return _coreNodeLabelSets.size(); }
    size_t edgeCount() const { return _edges.size(); }
    size_t getOutPatchEdgeCount() const { return _outPatchEdgeCount; };
    size_t getInPatchEdgeCount() const { return _inPatchEdgeCount; };

    void commit(JobSystem& jobSystem);

private:
    friend ConcurrentWriter;
    friend DataPart;

    EntityID _firstNodeID {0};
    EntityID _firstEdgeID {0};
    EntityID _nextNodeID {0};
    EntityID _nextEdgeID {0};
    Graph* _graph {nullptr};
    size_t _outPatchEdgeCount {0};
    size_t _inPatchEdgeCount {0};

    std::vector<LabelSetID> _coreNodeLabelSets;
    std::vector<EdgeRecord> _edges;
    std::unordered_map<EntityID, const EdgeRecord*> _patchedEdges;
    std::unordered_set<EntityID> _nodeHasPatchEdges;
    std::map<EntityID, LabelSetID> _patchNodeLabelSets;
    std::unique_ptr<PropertyManager> _nodeProperties;
    std::unique_ptr<PropertyManager> _edgeProperties;

    std::vector<LabelSetID>& coreNodeLabelSets() { return _coreNodeLabelSets; }
    std::vector<EdgeRecord>& edges() { return _edges; }
    std::unique_ptr<PropertyManager>& nodeProperties() { return _nodeProperties; }
    std::unique_ptr<PropertyManager>& edgeProperties() { return _edgeProperties; }
    std::map<EntityID, LabelSetID>& patchNodeLabelSets() { return _patchNodeLabelSets; }
    std::unordered_map<EntityID, const EdgeRecord*>& patchedEdges() { return _patchedEdges; }
    size_t patchNodeEdgeDataCount() const {
        return _nodeHasPatchEdges.size();
    }
};
}

