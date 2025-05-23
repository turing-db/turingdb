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
#include "versioning/VersionController.h"

namespace db {

class Commit;
class PropertyManager;

class CommitLoader {
public:
    [[nodiscard]] static DumpResult<std::unique_ptr<Commit>> load(const fs::Path& path, Graph& graph, CommitHash hash) {
        // Listing files in the folder
        auto files = path.listDir();
        if (!files) {
            return DumpError::result(DumpErrorType::CANNOT_LIST_DATAPARTS, files.error());
        }

        static constexpr std::string_view DATAPART_FOLDER_PREFIX = "datapart-";

        auto& versionController = graph._versionController;

        auto commit = std::make_unique<Commit>();
        commit->_graph = &graph;
        commit->_data = versionController->createCommitData(hash);
        commit->_data->_hash = hash;

        auto& metadata = commit->_data->_metadata;

        // Loading metadata
        {
            const fs::Path metadataPath = path / "metadata";
            auto res = GraphMetadataLoader::load(path, metadata);

            if (!res) {
                return res.get_unexpected();
            }
        }


        std::map<uint64_t, WeakArc<DataPart>> dataparts;
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

            auto res = DataPartLoader::load(child, metadata, *versionController);

            if (!res) {
                return res.get_unexpected();
            }

            WeakArc<DataPart> part = res.value();
            dataparts.emplace(partIndex.value(), part);
            graph.allocIDRange(part->getNodeCount(), part->getEdgeCount());
        }

        auto& history = commit->history();
        for (auto& [partIndex, part] : dataparts) {
            history.pushCommitDatapart(part);
        }

        return std::move(commit);
    }
};

}
