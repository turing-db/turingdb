#pragma once

#include "views/EdgeView.h"
#include "views/GraphView.h"
#include "iterators/GetPropertiesIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetNodeViewsIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanEdgePropertiesIterator.h"
#include "iterators/ScanInEdgesByLabelIterator.h"
#include "iterators/ScanNodePropertiesIterator.h"
#include "iterators/ScanNodePropertiesByLabelIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ScanOutEdgesByLabelIterator.h"
#include "iterators/MatchLabelSetIterator.h"

namespace db {

class GraphReader {
public:
    explicit GraphReader(const GraphView& view)
        : _view(view)
    {
    }


    bool isValid() const {
        return _view.isValid();
    }

    [[nodiscard]] const GraphMetadata& getMetadata() const;
    [[nodiscard]] size_t getNodeCount() const;
    [[nodiscard]] size_t getEdgeCount() const;
    [[nodiscard]] const GraphView& getView() const { return _view; }
    [[nodiscard]] DataPartSpan dataparts() const { return _view.dataparts(); }
    [[nodiscard]] std::span<const CommitView> commits() const { return _view.commits(); }
    [[nodiscard]] const EdgeRecord* getEdge(EdgeID edgeID) const;
    [[nodiscard]] LabelSetHandle getNodeLabelSet(NodeID nodeID) const;
    [[nodiscard]] size_t getNodeCountMatchingLabelset(const LabelSetHandle& labelset) const;
    [[nodiscard]] size_t getDatapartCount() const;
    [[nodiscard]] size_t getNodePropertyCount(PropertyTypeID ptID) const;
    [[nodiscard]] size_t getNodePropertyCount(size_t datapartIndex, PropertyTypeID ptID) const;
    [[nodiscard]] NodeID getFinalNodeID(size_t partIndex, NodeID tmpID) const;
    [[nodiscard]] NodeView getNodeView(NodeID id) const;
    [[nodiscard]] EdgeView getEdgeView(EdgeID id) const;
    [[nodiscard]] EdgeTypeID getEdgeTypeID(EdgeID edgeID) const;
    [[nodiscard]] GetNodeViewsRange getNodeViews(const ColumnNodeIDs* inputNodeIDs) const;
    [[nodiscard]] GetOutEdgesRange getOutEdges(const ColumnNodeIDs* inputNodeIDs) const;
    [[nodiscard]] GetInEdgesRange getInEdges(const ColumnNodeIDs* inputNodeIDs) const;
    [[nodiscard]] ScanEdgesRange scanOutEdges() const;
    [[nodiscard]] ScanNodesRange scanNodes() const;
    [[nodiscard]] ScanNodesByLabelRange scanNodesByLabel(const LabelSetHandle& labelset) const;
    [[nodiscard]] ScanOutEdgesByLabelRange scanOutEdgesByLabel(const LabelSetHandle& labelset) const;
    [[nodiscard]] ScanInEdgesByLabelRange scanInEdgesByLabel(const LabelSetHandle& labelset) const;
    [[nodiscard]] MatchLabelSetIterator matchLabelSets(const LabelSetHandle& labelSet) const;
    [[nodiscard]] bool nodeHasProperty(PropertyTypeID ptID, NodeID nodeID) const;
    [[nodiscard]] bool graphHasNode(NodeID nodeID) const;

    template <SupportedType T>
    [[nodiscard]] const T::Primitive* tryGetNodeProperty(PropertyTypeID ptID, NodeID nodeID) const;

    template <SupportedType T>
    [[nodiscard]] ScanNodePropertiesRange<T> scanNodeProperties(PropertyTypeID ptID) const {
        return ScanNodePropertiesRange<T>(_view, ptID);
    }

    template <SupportedType T>
    [[nodiscard]] ScanEdgePropertiesRange<T> scanEdgeProperties(PropertyTypeID ptID) const {
        return ScanEdgePropertiesRange<T>(_view, ptID);
    }

    template <SupportedType T>
    [[nodiscard]] ScanNodePropertiesByLabelRange<T> scanNodePropertiesByLabel(PropertyTypeID ptID,
                                                                              const LabelSetHandle& labelset) const {
        return ScanNodePropertiesByLabelRange<T>(_view, ptID, labelset);
    }

    template <SupportedType T>
    [[nodiscard]] GetNodePropertiesRange<T> getNodeProperties(PropertyTypeID ptID,
                                                              const ColumnNodeIDs* inputNodeIDs) const {
        return GetNodePropertiesRange<T>(_view, ptID, inputNodeIDs);
    }

private:
    GraphView _view;
};

}
