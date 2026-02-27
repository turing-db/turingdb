#include "CommitLoader.h"

#include "FilePageReader.h"
#include "DataPartLoader.h"
#include "GraphMetadataLoader.h"
#include "CommitJournalLoader.h"
#include "GraphDumpHelper.h"
#include "DumpResult.h"
#include "DumpConfig.h"

#include "DataPart.h"
#include "Graph.h"
#include "Path.h"
#include "dump/TombstonesLoader.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitHistoryBuilder.h"
#include "versioning/VersionController.h"

#include "BioAssert.h"

using namespace db;

DumpResult<std::unique_ptr<Commit>> CommitLoader::load(const fs::Path& commitDir,
                                                       const fs::Path& partsDir,
                                                       Graph& graph,
                                                       CommitHash hash,
                                                       const Commit* prevCommit) {
    Profile profile("CommitLoader::load");

    VersionController* controller = graph._versionController.get();

    std::unique_lock<std::shared_mutex> uniqueLock = controller->lock();
    VersionController::DataPartMap& partMap = controller->getPartMap();

    auto commit = std::make_unique<Commit>(
        controller,
        controller->createCommitData(hash),
        prevCommit);

    CommitHistoryBuilder historyBuilder = CommitHistoryBuilder(commit->_data->_history);

    auto& metadata = commit->_data->_metadata;

    // Loading metadata
    {
        Profile profile("CommitLoader::load <metadata>");

        auto res = GraphMetadataLoader::load(commitDir, metadata);

        if (!res) {
            return res.get_unexpected();
        }
    }

    // Reading journal
    {
        CommitJournal* journal = commit->_data->_history._journal.get();
        bioassert(journal, "invalid journal"); // Should be initialised in commit constructor

        const fs::Path journalFile = commitDir / "journal";

        auto readerRes = fs::FilePageReader::open(journalFile, DumpConfig::PAGE_SIZE);
        if (!readerRes) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_JOURNAL,
                                     readerRes.error());
        }

        CommitJournalLoader loader(readerRes.value());
        if (auto res = loader.load(*journal); !res) {
            return res.get_unexpected();
        }
    }

    // Reading tombstones
    {
        Tombstones& tombstones = commit->_data->_tombstones;

        const fs::Path tombstonesFile = commitDir / "tombstones";

        auto readerRes =
            fs::FilePageReader::open(tombstonesFile, DumpConfig::PAGE_SIZE);
        if (!readerRes) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_TOMBSTONES,
                                     readerRes.error());
        }

        TombstonesLoader loader(readerRes.value());
        if (auto res = loader.load(tombstones); !res) {
            return res.get_unexpected();
        }
    }

    // Load metadata
    {
        const fs::Path metadataFile = commitDir / "metadata";

        auto fileRes = fs::File::open(metadataFile);
        if (!fileRes) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_COMMIT_METADATA,
                                     fileRes.error());
        }

        fs::FileReader reader;
        reader.setFile(&fileRes.value());

        reader.read();
        if (reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_COMMIT_METADATA,
                                     *reader.error());
        }

        fs::ByteBufferIterator it = reader.iterateBuffer();

        if (auto res = GraphDumpHelper::checkFileHeader(&it); !res) {
            return res.get_unexpected();
        }

        const size_t numDataParts = it.get<uint64_t>();

        for (size_t i = 0; i < numDataParts; ++i) {
            const auto dataPartID = it.get<uint64_t>();
            const auto it = partMap.find(DataPartID(dataPartID));

            // If the datapart has already been read pass
            // the existing reference to it.
            if (it != partMap.end()) {
                historyBuilder.addDatapart(it->second);
                continue;
            }

            const fs::Path partDir = partsDir / std::to_string(dataPartID);

            auto res = DataPartLoader::load(partDir,
                                            metadata,
                                            controller);

            if (!res) {
                return res.get_unexpected();
            }

            historyBuilder.addDatapart(res.value());
            partMap.emplace(dataPartID, res.value());
        }

        historyBuilder.setCommitDatapartCount(it.get<size_t>());
        reader.file().close();
    }

    // Add Commit Metadata
    {
        const size_t numCommitDataParts = commit->data().commitDataparts().size();
        size_t numNodesAdded = 0;
        size_t numEdgesAdded = 0;

        for (size_t i = 0; i < numCommitDataParts; ++i) {
            const DataPart* part = commit->data().commitDataparts()[i].get();
            numNodesAdded += part->getNodeContainerSize();
            numEdgesAdded += part->getEdgeContainerSize();
        }

        commit->_numDataParts = numCommitDataParts;
        commit->_numNodes = numNodesAdded;
        commit->_numEdges = numEdgesAdded;

        commit->_prevCommit = prevCommit;
    }

    return std::move(commit);
}
