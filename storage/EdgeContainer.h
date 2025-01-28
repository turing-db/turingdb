#pragma once

#include <unordered_map>
#include <memory>
#include <span>

#include "EdgeRecord.h"

namespace db {

class NodeEdgeView;
class NodeContainer;
class DataPartLoader;
class EdgeIndexer;
class EdgeContainerLoader;

class EdgeContainer {
public:
    using EdgeRecords = std::vector<EdgeRecord>;

    EdgeContainer(const EdgeContainer&) = delete;
    EdgeContainer(EdgeContainer&&) noexcept;
    EdgeContainer& operator=(const EdgeContainer&) = delete;
    EdgeContainer& operator=(EdgeContainer&&) noexcept;
    ~EdgeContainer();

    static std::unique_ptr<EdgeContainer> create(EntityID firstNodeID,
                                                 EntityID firstEdgeID,
                                                 std::vector<EdgeRecord>&& outs);

    EntityID getFirstEdgeID() const { return _firstEdgeID; }
    EntityID getFirstNodeID() const { return _firstNodeID; }
    size_t size() const { return _outEdges.size(); }

    std::span<const EdgeRecord> getOuts() const {
        return _outEdges;
    }

    std::span<const EdgeRecord> getIns() const {
        return _inEdges;
    }

    std::span<const EdgeRecord> getOuts(size_t first, size_t count) const {
        return std::span{_outEdges}.subspan(first, count);
    }

    std::span<const EdgeRecord> getIns(size_t first, size_t count) const {
        return std::span{_inEdges}.subspan(first, count);
    }

    const EdgeRecord& get(EntityID edgeID) const {
        const size_t offset = (edgeID - _firstEdgeID).getValue();
        return _outEdges[offset];
    }

    const EdgeRecord* tryGet(EntityID edgeID) const {
        const size_t offset = (edgeID - _firstEdgeID).getValue();
        if (offset >= _outEdges.size()) {
            return nullptr;
        }

        return &_outEdges[offset];
    }

    const std::unordered_map<EntityID, EntityID>& getTmpToFinalEdgeIDs() const {
        return _tmpToFinalEdgeIDs;
    }

private:
    friend EdgeIndexer;
    friend DataPartLoader;
    friend EdgeContainerLoader;

    EntityID _firstNodeID = 0;
    EntityID _firstEdgeID = 0;

    EdgeRecords _outEdges;
    EdgeRecords _inEdges;

    // TODO Remove for the new Unique ID system
    std::unordered_map<EntityID, EntityID> _tmpToFinalEdgeIDs;

    EdgeContainer(EntityID firstNodeID,
                  EntityID firstEdgeID,
                  std::vector<EdgeRecord>&& outEdges,
                  std::vector<EdgeRecord>&& inEdges);
};

}
