#pragma once

#include "views/GraphView.h"

#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetNodeViewsIterator.h"
#include "iterators/GetPropertiesIterator.h"
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
    [[nodiscard]] GraphMetadata& getMetadata();
    [[nodiscard]] size_t getNodeCount() const;
    [[nodiscard]] size_t getEdgeCount() const;
    [[nodiscard]] const GraphView& getView() const { return _view; }
    [[nodiscard]] DataPartSpan dataparts() const { return _view._dataparts; }
    [[nodiscard]] const EdgeRecord* getEdge(EntityID edgeID) const;
    [[nodiscard]] LabelSetID getNodeLabelSetID(EntityID nodeID) const;
    [[nodiscard]] size_t getNodeCountMatchingLabelset(const LabelSet& labelset) const;
    [[nodiscard]] const LabelSet* getNodeLabelSet(EntityID nodeID) const;
    [[nodiscard]] size_t getDatapartCount() const;
    [[nodiscard]] size_t getNodePropertyCount(PropertyTypeID ptID) const;
    [[nodiscard]] size_t getNodePropertyCount(size_t datapartIndex, PropertyTypeID ptID) const;
    [[nodiscard]] EntityID getFinalNodeID(EntityID tmpID) const;
    [[nodiscard]] NodeView getNodeView(EntityID id) const;
    [[nodiscard]] EdgeView getEdgeView(EntityID id) const;
    [[nodiscard]] EdgeTypeID getEdgeTypeID(EntityID edgeID) const;
    [[nodiscard]] GetNodeViewsRange getNodeViews(const ColumnIDs* inputNodeIDs) const;
    [[nodiscard]] GetOutEdgesRange getOutEdges(const ColumnIDs* inputNodeIDs) const;
    [[nodiscard]] GetInEdgesRange getInEdges(const ColumnIDs* inputNodeIDs) const;
    [[nodiscard]] ScanEdgesRange scanOutEdges() const;
    [[nodiscard]] ScanNodesRange scanNodes() const;
    [[nodiscard]] ScanNodesByLabelRange scanNodesByLabel(const LabelSet* labelset) const;
    [[nodiscard]] ScanOutEdgesByLabelRange scanOutEdgesByLabel(const LabelSet* labelset) const;
    [[nodiscard]] ScanInEdgesByLabelRange scanInEdgesByLabel(const LabelSet* labelset) const;
    [[nodiscard]] MatchLabelSetIterator matchLabelSetIDs(const LabelSet* labelSet) const;
    [[nodiscard]] bool nodeHasProperty(PropertyTypeID ptID, EntityID nodeID) const;

    template <SupportedType T>
    [[nodiscard]] const T::Primitive* tryGetNodeProperty(PropertyTypeID ptID, EntityID nodeID) const;

    [[nodiscard]] PropertyType getPropertyType(const std::string& name) const;

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
                                                                       const LabelSet* labelset) const {
        return ScanNodePropertiesByLabelRange<T>(_view, ptID, labelset);
    }

    template <SupportedType T>
    [[nodiscard]] GetNodePropertiesRange<T> getNodeProperties(PropertyTypeID ptID,
                                                               const ColumnIDs* inputNodeIDs) const {
        return GetNodePropertiesRange<T>(_view, ptID, inputNodeIDs);
    }

private:
    GraphView _view;
};

}
