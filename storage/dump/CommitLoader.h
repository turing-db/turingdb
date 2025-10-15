#pragma once

#include <map>
#include <memory>

#include "DataPart.h"
#include "DataPartLoader.h"
#include "Graph.h"
#include "GraphMetadataLoader.h"
#include "Path.h"
#include "GraphDumpHelper.h"
#include "DumpResult.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitHistoryBuilder.h"
#include "versioning/VersionController.h"

namespace db {

class Commit;
class PropertyManager;

class CommitLoader {
public:
    [[nodiscard]] static DumpResult<std::unique_ptr<Commit>> load(const fs::Path& path,
                                                                  Graph& graph,
                                                                  CommitHash hash,
                                                                  const CommitHistory* prevHistory) {
        Profile profile {"CommitLoader::load"};

        // Listing files in the folder
        auto files = path.listDir();
        if (!files) {
            return DumpError::result(DumpErrorType::CANNOT_LIST_DATAPARTS, files.error());
        }

        static constexpr std::string_view DATAPART_FOLDER_PREFIX = "datapart-";

        auto& versionController = graph._versionController;

        auto commit = std::make_unique<Commit>(
            graph._versionController.get(),
            versionController->createCommitData(hash));

        if (prevHistory) {
            commit->_data->_history.newCommitHistoryFromPrevious(*prevHistory);
        }
        commit->_data->_history.pushCommit(commit->view());

        CommitHistoryBuilder historyBuilder {commit->_data->_history};

        auto& metadata = commit->_data->_metadata;

        // Loading metadata
        {
            Profile profile {"CommitLoader::load <metadata>"};

            const fs::Path metadataPath = path / "metadata";
            auto res = GraphMetadataLoader::load(path, metadata);

            if (!res) {
                return res.get_unexpected();
            }
        }

        std::map<uint64_t, fs::Path> datapartPaths;
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

            datapartPaths.emplace(partIndex.value(), child);
        }

        for (auto& [partIndex, path] : datapartPaths) {
            auto res = DataPartLoader::load(path, metadata, *versionController);

            if (!res) {
                return res.get_unexpected();
            }

            historyBuilder.addDatapart(res.value());
        }

        historyBuilder.setCommitDatapartCount(datapartPaths.size());

        return std::move(commit);
    }
};

}
