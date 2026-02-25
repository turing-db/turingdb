#pragma once

#include <map>
#include <memory>

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

namespace db {

class Commit;
class PropertyManager;

class CommitLoader {
public:
    [[nodiscard]] static DumpResult<std::unique_ptr<Commit>> load(const fs::Path& commitDir,
                                                                  Graph& graph,
                                                                  CommitHash hash,
                                                                  const Commit* prevCommit) {
        Profile profile("CommitLoader::load");

        // Listing files in the folder
        auto files = commitDir.listDir();
        if (!files) {
            return DumpError::result(DumpErrorType::CANNOT_LIST_DATAPARTS, files.error());
        }

        static constexpr std::string_view DATAPART_FOLDER_PREFIX = "datapart-";

        auto& versionController = graph._versionController;

        auto commit = std::make_unique<Commit>(
            graph._versionController.get(),
            versionController->createCommitData(hash),
            prevCommit);

        if (prevCommit) {
            commit->_data->_history.newCommitHistoryFromPrevious(prevCommit->history());
        }

        const auto it = std::ranges::find_if(files.value(),
                                             [&](const fs::Path& file) {
                                                 return file.filename() == "merge";
                                             });

        if (it != files->end()) {
            commit->_data->_history._allDataparts = {};
        }

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

        std::map<uint64_t, fs::Path> datapartDirs;
        for (auto& child : files.value()) {
            const auto& childStr = child.filename();

            if (childStr.find(DATAPART_FOLDER_PREFIX) == std::string::npos) {
                // Not a datapart folder
                continue;
            }

            const auto partIndex = GraphDumpHelper::getIntegerSuffix(
                childStr,
                DATAPART_FOLDER_PREFIX.size());

            if (!partIndex) {
                return partIndex.get_unexpected();
            }

            datapartDirs.emplace(partIndex.value(), child);
        }

        for (auto& [partIndex, dir] : datapartDirs) {
            auto res = DataPartLoader::load(dir, metadata, *versionController);

            if (!res) {
                return res.get_unexpected();
            }

            historyBuilder.addDatapart(res.value());
        }

        historyBuilder.setCommitDatapartCount(datapartDirs.size());

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

            commit->setNumDataParts(numCommitDataParts);
            commit->setNumNodes(numNodesAdded);
            commit->setNumEdges(numEdgesAdded);

            commit->_prevCommit = prevCommit;
        }

        return std::move(commit);
    }
};
}
