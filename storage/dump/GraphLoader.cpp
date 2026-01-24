#include "GraphLoader.h"

#include "CommitLoader.h"
#include "GraphInfoLoader.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"
#include "versioning/CommitView.h"

using namespace db;

DumpResult<void> GraphLoader::load(Graph* graph, const fs::Path& path) {
    Profile profile {"GraphLoader::load"};

    const auto pathInfo = path.getFileInfo();
    if (!pathInfo) {
        return DumpError::result(DumpErrorType::GRAPH_DOES_NOT_EXIST);
    }

    if (pathInfo->_type != fs::FileType::Directory) {
        return DumpError::result(DumpErrorType::NOT_DIRECTORY);
    }

    // Loading info
    {
        Profile profile {"GraphLoader::load <info>"};
        const fs::Path infoPath = path / "info";
        auto reader = fs::FilePageReader::open(infoPath, DumpConfig::PAGE_SIZE);
        if (!reader) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, reader.error());
        }

        GraphInfoLoader loader {reader.value()};

        auto res = loader.load(*graph);
        if (!res) {
            return res.get_unexpected();
        }
    }

    // Listing files in the folder
    auto files = path.listDir();
    if (!files) {
        return DumpError::result(DumpErrorType::CANNOT_LIST_COMMITS, files.error());
    }

    static constexpr std::string_view COMMIT_FOLDER_PREFIX = "commit-";

    graph->_versionController = std::make_unique<VersionController>(graph);

    std::map<uint64_t, std::pair<CommitHash, fs::Path>> commitInfo;

    for (auto& child : files.value()) {
        const auto& childStr = child.filename();

        if (childStr.find(COMMIT_FOLDER_PREFIX) == std::string::npos) {
            // Not a commit folder
            continue;
        }

        const auto suffixRes = GraphDumpHelper::getCommitSuffix(
            childStr,
            COMMIT_FOLDER_PREFIX.size());

        if (!suffixRes) {
            return suffixRes.get_unexpected();
        }

        const auto [offset, hash] = suffixRes.value();
        commitInfo.emplace(offset, std::make_pair(hash, child));
    }

    if (commitInfo.empty()) {
        return DumpError::result(DumpErrorType::NO_COMMITS);
    }

    const CommitHistory* prevHistory = nullptr;

    for (const auto& [commitIndex, commitInfoPair] : commitInfo) {
        const auto& [hash, path] = commitInfoPair;
        auto res = CommitLoader::load(path, *graph, CommitHash {hash}, prevHistory);

        if (!res) {
            return res.get_unexpected();
        }

        auto* ptr = res->get();
        graph->_versionController->addCommit(std::move(res.value()));
        prevHistory = &ptr->history();
    }

    return {};
}
