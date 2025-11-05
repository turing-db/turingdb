#pragma once

#include <limits>

#include "DataPartRebaser.h"
#include "EntityIDRebaser.h"
#include "MetadataRebaser.h"
#include "ID.h"

namespace db {

class CommitBuilder;
class CommitData;
class CommitHistory;
class ConflictCheckSets;
class Change;
class GraphReader;
class CommitView;
class Tombstones;

class ChangeRebaser {
public:
    ChangeRebaser(Change& change,
                  const CommitData* currentHeadCommitData,
                  const CommitHistory* currentHeadHistory);

    /**
     * @brief Performs the required setup tasks, ready to call @ref checkConflicts,
     * abstracting much logic out of the constructor.
     */
    void init(const GraphReader& mainReader, const GraphReader& branchTimeReader);

    /**
    * @brief Constructs a ChangeConflictChecker to compare against @param commits
    */
    void checkConflicts(std::span<const CommitView> commits);

    /**
     * @brief Rebases the provided @param commitBuilder according to the views of @ref
     * _branchTimeReader and @ref _newMainReader
     */
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
