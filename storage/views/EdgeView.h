#pragma once

#include "EntityPropertyView.h"

namespace db {

class GraphReader;

class EdgeView {
public:
    EdgeView(const EdgeView&) = delete;
    EdgeView(EdgeView&&) noexcept = default;
    EdgeView& operator=(const EdgeView&) = delete;
    EdgeView& operator=(EdgeView&&) noexcept = default;
    ~EdgeView() = default;

    const EntityPropertyView& properties() const { return _props; }
    EdgeID id() const { return _edgeID; }
    NodeID sourceID() const { return _srcID; }
    NodeID targetID() const { return _tgtID; }
    EdgeTypeID edgeTypeID() const { return _typeID; }

    bool isValid() const { return _edgeID.isValid(); }

private:
    friend GraphReader;

    EdgeID _edgeID;
    NodeID _srcID;
    NodeID _tgtID;
    EntityPropertyView _props;
    EdgeTypeID _typeID;
    EdgeView() = default;
};

}

