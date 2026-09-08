#pragma once

#include <unordered_set>

#include "datapart/DataPartSpan.h"
#include "versioning/CommitData.h"

#include "ID.h"

namespace db {

class GraphReader;
class Graph;

class GraphView {
public:
    using DeletedNodes = std::unordered_set<NodeID>;
    using DeletedEdges = std::unordered_set<EdgeID>;

    GraphView() = default;

    explicit GraphView(const CommitData* data)
        : _data(data)
    {
    }

    bool isValid() const { return _data; }

    // The nodes and edges a change has deleted and not committed. A read in that change
    // skips them as it skips the commit's tombstones, and they grow as the change deletes.
    void setChangeDeletions(const DeletedNodes* nodes, const DeletedEdges* edges);

    template <TypedInternalID IDT>
    [[nodiscard]] bool isDeleted(IDT id) const;

    [[nodiscard]] bool hasDeletedNodes() const;
    [[nodiscard]] bool hasDeletedEdges() const;
    [[nodiscard]] bool followsChangeDeletions() const { return _changeDeletedEdges != nullptr; }

    [[nodiscard]] GraphReader read() const;
    [[nodiscard]] DataPartSpan dataparts() const { return _data->allDataparts(); }
    [[nodiscard]] CommitHash headCommitHash() const { return _data->hash(); }
    [[nodiscard]] DataPartSpan commitDataparts() const { return _data->commitDataparts(); }
    [[nodiscard]] const Tombstones& tombstones() const { return _data->tombstones(); }
    [[nodiscard]] const GraphMetadata& metadata() const { return _data->metadata(); }
    [[nodiscard]] EdgeBranchingCache& branchingCache() const { return _data->branchingCache(); }
    [[nodiscard]] const CommitHistory& history() const { return _data->history(); }
    std::span<const WeakArc<Index>> indexes() const { return _data->indexes(); }

private:
    friend GraphReader;
    const CommitData* _data {nullptr};
    const DeletedNodes* _changeDeletedNodes {nullptr};
    const DeletedEdges* _changeDeletedEdges {nullptr};
};

}
