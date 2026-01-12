#include "EdgeContainer.h"

#include <range/v3/action/sort.hpp>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/zip.hpp>

#include "Profiler.h"

namespace rv = ranges::views;
namespace rg = ranges;

namespace db {

EdgeContainer::EdgeContainer(EdgeContainer&&) noexcept = default;
EdgeContainer& EdgeContainer::operator=(EdgeContainer&&) noexcept = default;
EdgeContainer::~EdgeContainer() = default;

std::unique_ptr<EdgeContainer> EdgeContainer::create(NodeID firstNodeID,
                                                     EdgeID firstEdgeID,
                                                     std::vector<EdgeRecord>&& outs,
                                                     std::unordered_map<EdgeID, EdgeID>& tmpToFinalEdgeIDs) {
    Profile profile {"EdgeContainer::create"};

    // Sort out edges based on the source node
    rg::sort(outs, [&](const EdgeRecord& a, const EdgeRecord& b) {
        return a._nodeID < b._nodeID;
    });

    // New edge IDs
    for (const auto& [i, out] : outs | rv::enumerate) {
        const EdgeID finalOutID = EdgeID {firstEdgeID + i};
        tmpToFinalEdgeIDs[out._edgeID] = finalOutID;
        out._edgeID = finalOutID;
    }

    // In edges are just a copy of out edges
    std::vector<EdgeRecord> ins = outs
                                | rv::transform([&](const EdgeRecord& out) {
                                      return EdgeRecord {
                                          ._edgeID = out._edgeID,
                                          ._nodeID = out._otherID,
                                          ._otherID = out._nodeID,
                                          ._edgeTypeID = out._edgeTypeID,
                                      };
                                  })
                                | rg::to_vector;

    // Sort in edges based on the target node
    rg::sort(ins, [&](const EdgeRecord& a, const EdgeRecord& b) {
        return a._nodeID < b._nodeID;
    });

    auto* ptr = new EdgeContainer(firstNodeID,
                                  firstEdgeID,
                                  std::move(outs),
                                  std::move(ins));
    auto edges = std::unique_ptr<EdgeContainer>(ptr);

    return edges;
}

EdgeContainer::EdgeContainer(NodeID firstNodeID,
                             EdgeID firstEdgeID,
                             std::vector<EdgeRecord>&& outEdges,
                             std::vector<EdgeRecord>&& inEdges)
    : _firstNodeID(firstNodeID),
      _firstEdgeID(firstEdgeID),
      _outEdges(std::move(outEdges)),
      _inEdges(std::move(inEdges))
{
}

}
