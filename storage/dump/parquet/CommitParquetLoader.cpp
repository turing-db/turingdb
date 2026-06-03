#include "CommitParquetLoader.h"

#include <stddef.h>

#include "GraphMetadataParquetLoader.h"
#include "CommitJournalParquetLoader.h"
#include "TombstonesParquetLoader.h"
#include "CommitMetaDataParquetLoader.h"
#include "DataPartParquetLoader.h"

#include "datapart/DataPart.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSetMap.h"
#include "versioning/Commit.h"
#include "versioning/CommitData.h"
#include "versioning/CommitHistory.h"
#include "versioning/CommitHistoryBuilder.h"
#include "versioning/CommitJournal.h"
#include "versioning/DataPartID.h"
#include "versioning/Tombstones.h"
#include "versioning/VersionController.h"
#include "Path.h"

#include "BioAssert.h"

using namespace db;

std::unique_ptr<Commit> CommitParquetLoader::load(VersionController* controller,
                                                  CommitHash hash,
                                                  const fs::Path& commitDir,
                                                  const Commit* prevCommit) {
    auto commit = std::make_unique<Commit>(controller, hash, prevCommit);

    CommitParquetMetaData metadata;
    CommitMetaDataParquetLoader::load(commitDir, metadata);

    commit->_numNodes = metadata._numNodes;
    commit->_numEdges = metadata._numEdges;
    commit->_numDataParts = metadata._numCommitDataParts;

    return commit;
}

void CommitParquetLoader::loadData(const fs::Path& commitDir,
                                   const fs::Path& partsDir,
                                   VersionController* controller,
                                   Commit* commit) {
    VersionController::DataPartMap& partMap = controller->getPartMap();
    commit->setCommitData(controller->createCommitData(commit->hash()));

    CommitHistoryBuilder historyBuilder {commit->_data->_history};

    GraphMetadata& metadata = commit->_data->_metadata;
    GraphMetadataParquetLoader::load(commitDir, metadata);

    CommitJournal* journal = commit->_data->_history._journal.get();
    bioassert(journal, "invalid journal"); // Should be initialised in commit constructor
    CommitJournalParquetLoader::load(commitDir, *journal);

    Tombstones& tombstones = commit->_data->_tombstones;
    TombstonesParquetLoader::load(commitDir, tombstones);

    CommitParquetMetaData commitMetaData;
    CommitMetaDataParquetLoader::load(commitDir, commitMetaData);

    const LabelSetMap& labelsets = metadata.labelsets();

    for (const DataPartID dataPartID : commitMetaData._allDatapartIds) {
        const auto existing = partMap.find(dataPartID);

        // If the datapart has already been read, share the existing reference.
        if (existing != partMap.end()) {
            historyBuilder.addDatapart(existing->second);
            continue;
        }

        WeakArc<DataPart> part = controller->createDataPart(NodeID {0}, EdgeID {0}, dataPartID);
        const fs::Path partDir = partsDir / std::to_string(dataPartID.get());

        DataPartParquetLoader::load(*part, partDir, labelsets);

        historyBuilder.addDatapart(part);
        partMap.emplace(dataPartID, part);
    }

    historyBuilder.setCommitDatapartCount(commitMetaData._numCommitDataParts);
}
