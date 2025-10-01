#pragma once

#include <vector>

#include "ID.h"
#include "NodeRange.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

/**
 * @brief Stores information as to the modifications (updates/deletes) that occured in
 * this commit. That is, the updates and deletes which, when applied to the last commit,
 * resulted in the state of this commit.
 * @detail The first commit always contains an empty journal.
 */
class CommitJournal {
public:
    /**
     * @brief Creates a new journal entry which outlines the deletions specified by the
     * provided @param wb
     */
    [[nodiscard]] static CommitJournal* newJournal(const CommitWriteBuffer& wb);
private:
    std::vector<NodeRange> _deletedNodeRanges;
    std::vector<EdgeID> _deletedEdges;
};
}
