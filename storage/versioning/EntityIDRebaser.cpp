#include "EntityIDRebaser.h"

#include "BioAssert.h"

using namespace db;

EntityIDRebaser::EntityIDRebaser() = default;
EntityIDRebaser::~EntityIDRebaser() = default;

EntityIDRebaser::EntityIDRebaser(const EntityIDRebaser& other) = default;
EntityIDRebaser& EntityIDRebaser::operator=(const EntityIDRebaser& other) = default;
EntityIDRebaser::EntityIDRebaser(EntityIDRebaser&& other) noexcept = default;
EntityIDRebaser& EntityIDRebaser::operator=(EntityIDRebaser&& other) noexcept = default;

// If a @ref Change makes commits locally, it will create nodes according to what it
// thinks the next NodeID should be. This view is determined by the next node ID at
// the time the change branched from main. In subsequent commits on the Change, it is
// possible that there have been edges created using NodeID injection, e.g.
// @code
// CREATE (n:NEWNODE) <- Node created in this Change
// COMMIT             <- NEWNODE is written, assume it is given ID 5
// CREATE (n @ 5)-[e:NEWEDGE]-(m:NEWNODE) <- Edge created in this change, from node 5
// @endcode
// There may have been other Changes which have submitted to main in the time between
// this Change branching and submitting. Thus, when this Change comes to submit, there
// may have been a node with ID 5 created on main. If this Change commits
// without rebasing, NEWEDGE will be between the node with ID 5 that was created in
// the other change, and submitted to main, and not the "local" node with ID 5 as it
// should. Hence, we need to compare:
// 1. the nextNodeID on main when this Change was created
// 2. the nextNodeID on main when this Change attempts to submit
// If they are different, there have been nodes created on main between Change
// creation and submission. We therefore need to readjust any edges which reference a
// NodeID which is greater than the nextNodeID at time of Change creation, as these
// are edges between locally created nodes.
NodeID EntityIDRebaser::rebaseNodeID(NodeID old) const {
    bioassert(_branchTimeNextNodeID <= _newNextNodeID, "invalid _branchTimeNextNodeID");
    return old >= _branchTimeNextNodeID ? old + _newNextNodeID - _branchTimeNextNodeID
                                        : old;
}

EdgeID EntityIDRebaser::rebaseEdgeID(EdgeID old) const {
    bioassert(_branchTimeNextEdgeID <= _newNextEdgeID, "invalid _branchTimeNextEdgeID");
    return old >= _branchTimeNextEdgeID ? old + _newNextEdgeID - _branchTimeNextEdgeID
                                        : old;
}
