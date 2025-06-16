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
class DataPartRebaser;

class EdgeContainer {
public:
    using EdgeRecords = std::vector<EdgeRecord>;

    EdgeContainer(const EdgeContainer&) = delete;
    EdgeContainer(EdgeContainer&&) noexcept;
    EdgeContainer& operator=(const EdgeContainer&) = delete;
    EdgeContainer& operator=(EdgeContainer&&) noexcept;
    ~EdgeContainer();

    static std::unique_ptr<EdgeContainer> create(NodeID firstNodeID,
                                                 EdgeID  firstEdgeID,
                                                 std::vector<EdgeRecord>&& outs);

    EdgeID getFirstEdgeID() const { return _firstEdgeID; }
    NodeID getFirstNodeID() const { return _firstNodeID; }
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

    const EdgeRecord& get(EdgeID edgeID) const {
        const size_t offset = (edgeID - _firstEdgeID).getValue();
        return _outEdges[offset];
    }

    const EdgeRecord* tryGet(EdgeID edgeID) const {
        const size_t offset = (edgeID - _firstEdgeID).getValue();
        if (offset >= _outEdges.size()) {
            return nullptr;
        }

        return &_outEdges[offset];
    }

    const std::unordered_map<EdgeID, EdgeID>& getTmpToFinalEdgeIDs() const {
        return _tmpToFinalEdgeIDs;
    }

private:
    friend EdgeIndexer;
    friend DataPartLoader;
    friend EdgeContainerLoader;
    friend DataPartRebaser;

    NodeID  _firstNodeID = 0;
    EdgeID _firstEdgeID = 0;

    EdgeRecords _outEdges;
    EdgeRecords _inEdges;

    // TODO Remove for the new Unique ID system
    std::unordered_map<EdgeID, EdgeID> _tmpToFinalEdgeIDs;

    EdgeContainer(NodeID firstNodeID,
                  EdgeID firstEdgeID,
                  std::vector<EdgeRecord>&& outEdges,
                  std::vector<EdgeRecord>&& inEdges);
};

}
