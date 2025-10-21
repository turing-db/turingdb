#pragma once

#include "ID.h"

namespace db {

class EntityIDRebaser {
public:
    EntityIDRebaser();
    EntityIDRebaser(NodeID branchTimeNextNodeID,
                    EdgeID branchTimeNextEdgeID,
                    NodeID newNextNodeID,
                    EdgeID newNextEdgeID)
    : _branchTimeNextNodeID(branchTimeNextNodeID),
    _branchTimeNextEdgeID(branchTimeNextEdgeID),
    _newNextNodeID(newNextNodeID),
    _newNextEdgeID(newNextEdgeID)
    {
    }

    ~EntityIDRebaser();

    EntityIDRebaser(const EntityIDRebaser& other);
    EntityIDRebaser& operator=(const EntityIDRebaser& other);

    EntityIDRebaser(EntityIDRebaser&& other) noexcept;
    EntityIDRebaser& operator=(EntityIDRebaser&& other) noexcept;

    NodeID rebaseNodeID(NodeID old);
    EdgeID rebaseEdgeID(EdgeID old);

private:
    NodeID _branchTimeNextNodeID {std::numeric_limits<NodeID>::max()};
    EdgeID _branchTimeNextEdgeID {std::numeric_limits<EdgeID>::max()};
    NodeID _newNextNodeID {std::numeric_limits<NodeID>::max()};
    EdgeID _newNextEdgeID {std::numeric_limits<EdgeID>::max()};
};
}
