#pragma once

#include "versioning/WriteSet.h"
#include "ID.h"

namespace db { class CommitWriteBuffer;

/**
 * @brief Stores information as to the modifications (updates/deletes) that occured in
 * this commit. That is, the updates and deletes which, when applied to the last commit,
 * resulted in the state of this commit.
 * @detail The first commit always contains an empty journal.
 */
class CommitJournal {
public:
    using NodeWriteSet = WriteSet<NodeID>;
    using EdgeWriteSet = WriteSet<EdgeID>;

    /**
     * @brief Creates a new journal with an empty write set
     */
    [[nodiscard]] static std::unique_ptr<CommitJournal> emptyJournal();

    void clear();
    bool empty();

    void addWrittenNode(NodeID node);
    void addWrittenEdge(EdgeID edge);

    void addWrittenNodes(const std::vector<NodeID>& nodes);
    void addWrittenEdges(const std::vector<EdgeID>& edges);

    auto& nodeWriteSet() { return _nodeWriteSet; }
    auto& edgeWriteSet() { return _edgeWriteSet; }

    const NodeWriteSet& nodeWriteSet() const { return _nodeWriteSet; }
    const EdgeWriteSet& edgeWriteSet() const { return _edgeWriteSet; }

    void finalise();

private:
    friend class CommitJournalLoader;

    bool _initialised {false};

    NodeWriteSet _nodeWriteSet;
    EdgeWriteSet _edgeWriteSet;

    CommitJournal() = default;

    std::vector<NodeID>& rawNodeWriteSet() { return _nodeWriteSet.getRaw(); }
    std::vector<EdgeID>& rawEdgeWriteSet() { return _edgeWriteSet.getRaw(); }
};
}
