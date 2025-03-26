#pragma once

#include <map>
#include <memory>

#include "DataPart.h"
#include "DataPartLoader.h"
#include "Graph.h"
#include "Path.h"
#include "GraphDumpHelper.h"
#include "DumpResult.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/VersionController.h"

namespace db {

class Commit;
class PropertyManager;
class GraphMetadata;

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
        commit->_data = versionController->createCommitData();
        commit->_data->_graphMetadata = graph.getMetadata();
        commit->_data->_hash = hash;

        std::map<uint64_t, WeakArc<const DataPart>> dataparts;
        for (auto& child : files.value()) {
            const auto& childStr = child.get();

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

            child = path / child.get();

            auto res = DataPartLoader::load(child, *graph.getMetadata(), *versionController);

            if (!res) {
                return res.get_unexpected();
            }

            WeakArc<const DataPart> part = res.value();
            dataparts.emplace(partIndex.value(), part);
            graph.allocIDRange(part->getNodeCount(), part->getEdgeCount());
        }

        for (auto& [partIndex, part] : dataparts) {
            commit->_data->_history._commitDataparts.emplace_back(part);
        }

        return std::move(commit);
    }
};

}
