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
    const Commit::CommitSpan _commits;

    ColumnVector<NodeID> _deletedExistingNodes;

    void getWritesSinceCommit(ConflictCheckSets& writes);

    void checkNewEdgesIncidentToDeleted(const CommitData& latestCommitData);

    void checkPendingEdgeConflicts(const ConflictCheckSets& writes, const CommitWriteBuffer& writeBuffer);

    void checkDeletedNodeConflicts(const ConflictCheckSets& writes, const CommitWriteBuffer& writeBuffer);
    void checkDeletedEdgeConflicts(const ConflictCheckSets& writes, const CommitWriteBuffer& writeBuffer);
};
    
}
