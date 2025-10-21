#pragma once

#include <limits>

#include "versioning/DataPartRebaser.h"
#include "versioning/EntityIDRebaser.h"
#include "versioning/MetadataRebaser.h"
#include "ID.h"

namespace db {

class CommitBuilder;
class CommitData;
class CommitHistory;
class ConflictCheckSets;
class Change;
class GraphReader;

class ChangeRebaser {
public:
    ChangeRebaser(Change& change,
                  const CommitData* currentHeadCommitData,
                  const CommitHistory* currentHeadHistory);

    void init(const GraphReader& mainReader, const GraphReader& branchTimeReader);

    void checkConflicts(const ConflictCheckSets& writes);

    void rebaseCommitBuilder(CommitBuilder& commitBuilder);

private:
    Change* _change {nullptr};
    const GraphReader* _branchTimeReader {nullptr};
    const GraphReader* _newMainReader {nullptr};
    const CommitData* _currentHeadCommitData {nullptr};
    const CommitHistory* _currentHeadHistory {nullptr};
    MetadataRebaser _metadataRebaser;
    DataPartRebaser _dataPartRebaser;
    EntityIDRebaser _entityIDRebaser;

    void checkPendingEdgeConflicts(const ConflictCheckSets& writes,
                                   const CommitWriteBuffer& writeBuffer);

    void checkDeletedNodeConflicts(const ConflictCheckSets& writes,
                                   const CommitWriteBuffer& writeBuffer);
    void checkDeletedEdgeConflicts(const ConflictCheckSets& writes,
                                   const CommitWriteBuffer& writeBuffer);

    NodeID _newNextNodeID {std::numeric_limits<NodeID>::max()};
    EdgeID _newNextEdgeID {std::numeric_limits<EdgeID>::max()};
};
    
}
