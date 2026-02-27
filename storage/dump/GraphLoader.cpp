#include "GraphLoader.h"

#include "CommitLoader.h"
#include "GraphInfoLoader.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"
#include "versioning/CommitView.h"

using namespace db;

DumpResult<void> GraphLoader::load(Graph* graph, const fs::Path& graphDir) {
    Profile profile("GraphLoader::load");

    const auto dirInfo = graphDir.getFileInfo();
    if (!dirInfo) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }

    if (dirInfo->_type != fs::FileType::Directory) {
        return DumpError::result(DumpErrorType::NOT_DIRECTORY);
    }

    // Loading info
    {
        Profile profile("GraphLoader::load <info>");
        const fs::Path infoFile = graphDir / "info";
        auto reader = fs::FilePageReader::open(infoFile, DumpConfig::PAGE_SIZE);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, reader.error());
        }

        GraphInfoLoader loader(reader.value());

        auto res = loader.load(*graph);
        if (!res) {
            return res.get_unexpected();
        }
    }

    graph->_versionController = std::make_unique<VersionController>(graph);

    const fs::Path logFile = graphDir / "commitlog";

    auto fileRes = fs::File::open(logFile);
    if (!fileRes) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_COMMIT_LOG, fileRes.error());
    }

    fs::FileReader reader;
    reader.setFile(&fileRes.value());
    reader.read();
    if (reader.errorOccured()) {
        return DumpError::result(DumpErrorType::CANNOT_OPEN_COMMIT_LOG);
    }
    fs::ByteBufferIterator it = reader.iterateBuffer();

    const auto fileHeaderRes = GraphDumpHelper::checkFileHeader(&it);
    if (!fileHeaderRes) {
        return fileHeaderRes.get_unexpected();
    }

    const size_t numEntries = it.get<uint64_t>();

    const Commit* prevCommit = nullptr;
    for (size_t i = 0; i < numEntries; ++i) {
        const auto hash = it.get<uint64_t>();
        const fs::Path commitDir = graphDir / "commits" / fmt::format("{}", hash);
        const fs::Path partDir = graphDir / "dataparts";

        auto res = CommitLoader::load(commitDir,
                                      partDir,
                                      *graph,
                                      CommitHash {hash},
                                      prevCommit);
        if (!res) {
            return res.get_unexpected();
        }

        prevCommit = res.value().get();
        graph->_versionController->addCommit(std::move(res.value()));
    }

    return {};
}
