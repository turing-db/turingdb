#pragma once

#include "versioning/WriteSet.h"
#include "ID.h"

namespace db {

class CommitWriteBuffer;

/**
 * @brief Stores information as to the modifications (updates/deletes) that occured in
 * this commit. That is, the updates and deletes which, when applied to the last commit,
 * resulted in the state of this commit.
 * @detail The first commit always contains an empty journal.
 */
class CommitJournal {
public:
    /**
     * @brief Creates a new journal entry based on the contents of the provided @param wb
     */
    [[nodiscard]] static std::unique_ptr<CommitJournal> newJournal(CommitWriteBuffer& wb);

    /**
     * @brief Creates a new journal with an empty write set
     */
    [[nodiscard]] static std::unique_ptr<CommitJournal> emptyJournal();

    void clear();
    bool empty();

    void addWrittenNodes(const std::vector<NodeID>& nodes);
    void addWrittenEdges(const std::vector<EdgeID>& edges);

private:
    bool _initialised {false};

    WriteSet<NodeID> _nodeWriteSet;
    WriteSet<EdgeID> _edgeWriteSet;

    CommitJournal() = default;
};
}
