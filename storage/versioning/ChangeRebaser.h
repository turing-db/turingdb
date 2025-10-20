#pragma once

#include "ID.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/MetadataRebaser.h"
#include <limits>

namespace db {

class CommitBuilder;
class CommitData;
class CommitHistory;
class ConflictCheckSets;
class Change;
class GraphReader;

class ChangeRebaser {
public:
    ChangeRebaser(Change& change, const CommitData* currentHeadCommitData,
                  const CommitHistory* currentHeadHistory);

    void initialise(const GraphReader& mainReader, const GraphReader& branchTimeReader);

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

    NodeID rebaseNodeID(NodeID old);
    EdgeID rebaseEdgeID(EdgeID old);

    void checkPendingEdgeConflicts(const ConflictCheckSets& writes,
                                   const CommitWriteBuffer& writeBuffer);

    void checkDeletedNodeConflicts(const ConflictCheckSets& writes,
                                   const CommitWriteBuffer& writeBuffer);
    void checkDeletedEdgeConflicts(const ConflictCheckSets& writes,
                                   const CommitWriteBuffer& writeBuffer);

    NodeID _branchTimeNextNodeID {std::numeric_limits<NodeID>::max()};
    EdgeID _branchTimeNextEdgeID {std::numeric_limits<EdgeID>::max()};
    NodeID _newNextNodeID {std::numeric_limits<NodeID>::max()};
    EdgeID _newNextEdgeID {std::numeric_limits<EdgeID>::max()};
};
    
}
