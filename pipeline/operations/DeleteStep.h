#pragma once

#include <set>

#include "ExecutionContext.h"
#include "versioning/CommitBuilder.h"
#include <ID.h>

namespace db {
    
class DeleteStep {
public:
    using NodeSet = std::set<NodeID>;
    using EdgeSet = std::set<EdgeID>;

    struct Tag {};

    explicit DeleteStep(NodeSet& nodesToDelete, EdgeSet& edgesToDelete)
        : _delNodes(nodesToDelete),
        _delEdges(edgesToDelete)
    {
    }

    DeleteStep(DeleteStep&& other) = default;
    ~DeleteStep() = default;

    void prepare(ExecutionContext* ctxt);

    void reset() {};

    bool isFinished() const { return true; }

    void execute();

    void describe(std::string& descr) const;

private:
    NodeSet& _delNodes;
    EdgeSet& _delEdges;

    CommitBuilder* _commitBuilder {nullptr};
    std::span<const WeakArc<DataPart>> _dps {}; // nullptr, size 0

    /**
     * @brief Identifies edges that must be deleted due to one of their incident nodes
     *        being deleted, and appends those EdgeIDs to @ref _edgesToDelete.
     */
    void detectHangingEdges(const WeakArc<DataPart>& oldDP, const NodeSet&& nodesToDelete,
                            EdgeSet& edgesToDelete);
};

}
