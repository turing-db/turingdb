#pragma once

#include "NodeEdgeView.h"
#include "EntityPropertyView.h"
#include "metadata/LabelSetHandle.h"

namespace db {

class GraphReader;

class NodeView {
public:
    NodeView() = default;
    NodeView(NodeView&&) noexcept = default;
    NodeView& operator=(NodeView&&) noexcept = default;
    NodeView(const NodeView&) = default;
    NodeView& operator=(const NodeView&) = default;

    ~NodeView() = default;

    NodeID nodeID() const { return _nodeID; }
    const EntityPropertyView& properties() const { return _props; }
    const NodeEdgeView& edges() const { return _edges; }
    const LabelSetHandle& labelset() const { return _labelset; }

    bool isValid() const { return _nodeID.isValid(); }

private:
    friend GraphReader;

    NodeID _nodeID;
    LabelSetHandle _labelset;
    EntityPropertyView _props;
    NodeEdgeView _edges;
};

}

