#pragma once

#include <limits>

#include "Commit.h"
#include "DataPartRebaser.h"
#include "EntityIDRebaser.h"
#include "MetadataRebaser.h"
#include "ID.h"

namespace db {

class CommitBuilder;
class CommitData;
class CommitHistory;
struct ConflictCheckSets;
class Change;
class GraphReader;

class ChangeRebaser {
public:
    ChangeRebaser(Change& change,
                  const CommitData* currentHeadCommitData,
                  const CommitHistory* currentHeadHistory);

    void init(const GraphReader& mainReader, const GraphReader& branchTimeReader);

    void checkConflicts(const Commit::CommitSpan commits);

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

    NodeID _newNextNodeID {std::numeric_limits<NodeID>::max()};
    EdgeID _newNextEdgeID {std::numeric_limits<EdgeID>::max()};

    /**
     * @brief Rebases the IDs of Nodes and Edges in @param
     * tombstones::_nodeTombstones/_edgeTombstones according to main. Performs set union
     * on the tombstones in main and @param tombstones.
     */
    void rebaseTombstones(Tombstones& tombstones);
};
    
}
