#include "GraphLoader.h"

#include "CommitLoader.h"
#include "GraphInfoLoader.h"
#include "GraphMetadataLoader.h"
#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/VersionController.h"
#include "versioning/CommitView.h"

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

    static constexpr std::string_view COMMIT_FOLDER_PREFIX = "commit-";

    graph->_versionController = std::make_unique<VersionController>();
    graph->_versionController->_dataManager = std::make_unique<ArcManager<CommitData>>();
    graph->_versionController->_partManager = std::make_unique<ArcManager<DataPart>>();

    std::map<uint64_t, std::unique_ptr<Commit>> commits;
    for (auto& child : files.value()) {
        const auto& childStr = child.get();

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

        child = path / child.get();

        const auto [offset, hash] = suffixRes.value();

        auto res = CommitLoader::load(child, *graph, CommitHash {hash});

        if (!res) {
            return suffixRes.get_unexpected();
        }

        commits.emplace(offset, std::move(res.value()));
    }

    if (commits.empty()) {
        return DumpError::result(DumpErrorType::NO_COMMITS);
    }

    for (auto& [commitIndex, commit] : commits) {
        auto& history = commit->history();

        if (commitIndex != 0) {
            const auto& prevCommit = commits.at(commitIndex - 1);
            const auto prevDataparts = prevCommit->_data->allDataparts();

            history.pushPreviousDataparts(prevDataparts);
            history.pushPreviousCommits(prevCommit->history().commits());
        }

        history.pushCommit(CommitView{commit.get()});
        history.pushPreviousDataparts(commit->history().commitDataparts());
    }

    for (auto& [commitIndex, commit] : commits) {
        graph->_versionController->commit(std::move(commit));
    }


    return {};
}
