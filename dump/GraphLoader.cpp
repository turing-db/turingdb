#include "GraphLoader.h"

#include "DataPartLoader.h"
#include "GraphInfoLoader.h"
#include "GraphMetadataLoader.h"
#include "Graph.h"

using namespace db;

DumpResult<void> GraphLoader::load(Graph* graph, const fs::Path& path) {

    auto pathInfo = path.getFileInfo();
    if (!pathInfo) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }

    if (pathInfo->_type != fs::FileType::Directory) {
        return DumpError::result(DumpErrorType::NOT_DIRECTORY);
    }

    // Loading info
    {
        const fs::Path infoPath = path / "info";
        auto reader = fs::FilePageReader::open(infoPath);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, reader.error());
        }

        GraphInfoLoader loader {reader.value()};

        auto res = loader.load(*graph);
        if (!res) {
            return res.get_unexpected();
        }
    }

    // Loading metadata
    auto metadata = GraphMetadataLoader::load(path);

    if (!metadata) {
        return metadata.get_unexpected();
    }

    graph->_metadata = std::move(metadata.value());

    // Listing files in the folder
    auto files = path.listDir();
    if (!files) {
        return DumpError::result(DumpErrorType::CANNOT_LIST_DATAPARTS, files.error());
    }

    // static constexpr std::string_view DATAPART_FOLDER_PREFIX = "datapart-";

    // size_t nodeCount = 0;
    // size_t edgeCount = 0;

    // std::map<uint64_t, std::unique_ptr<DataPart>> dataparts;
    // for (auto& child : files.value()) {
    //     const auto& childStr = child.get();

    //     if (childStr.find(DATAPART_FOLDER_PREFIX) == std::string::npos) {
    //         // Not a datapart folder
    //         continue;
    //     }

    //     const auto partIndex = GraphDumpHelper::getIntegerSuffix(
    //         childStr,
    //         DATAPART_FOLDER_PREFIX.size());

    //     if (!partIndex) {
    //         return partIndex.get_unexpected();
    //     }

    //     child = path / child.get();

    //     auto res = DataPartLoader::load(child, *graph->getMetadata());

    //     if (!res) {
    //         return res.get_unexpected();
    //     }

    //     nodeCount += (*res)->getNodeCount();
    //     edgeCount += (*res)->getEdgeCount();
    //     dataparts.emplace(partIndex.value(), std::move(res.value()));
    // }

    // for (auto& [partIndex, part] : dataparts) {
    //     graph->_parts->store(std::move(part));
    // }

    // graph->allocIDRange(nodeCount, edgeCount);

    return {};
}
