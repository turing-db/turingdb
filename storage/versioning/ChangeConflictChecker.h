#pragma once

#include "Commit.h"
#include "columns/ColumnVector.h"

namespace db {

class Change;
class CommitWriteBuffer;
class EntityIDRebaser;

class ChangeConflictChecker {
public:
    ChangeConflictChecker(const Change& change,
                          const EntityIDRebaser& entityRebaser,
                          const Commit::CommitSpan commits);

    void checkConflicts();

private:
    const Change& _change;
    const EntityIDRebaser& _entityIDRebaser;
    const Commit::CommitSpan _commitsSinceBranch;

    ColumnVector<NodeID> _deletedExistingNodes;

    void getWritesSinceCommit(ConflictCheckSets& writes);

    /**
     * @brief Checks for write conflicts which are the result of @ref _change attempting
     * to delete a node which has had edges created incident to it on main in the time
     * between branch and submit
     * TODO specialise for DETACH/NODETACH
     */
    void checkNewEdgesIncidentToDeleted(const CommitData& latestCommitData);

    /**
     * @brief Checks for write conflicts which are the result of trying to create an edge
     * between a node which has since been written to on main
     */
    void checkPendingEdgeConflicts(const ConflictCheckSets& writes, const CommitWriteBuffer& writeBuffer);

    /**
     * @brief Checks for write conflicts which are the result of trying to delete a node
     * which has since been written to on main
     */
    void checkDeletedNodeConflicts(const ConflictCheckSets& writes, const CommitWriteBuffer& writeBuffer);

    /**
     * @brief Checks for write conflicts which are the result of trying to delete an edge
     * which has since been written to main
     */
    void checkDeletedEdgeConflicts(const ConflictCheckSets& writes, const CommitWriteBuffer& writeBuffer);
};
    
}
