#include "GraphDumper.h"

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "GraphInfoDumper.h"
#include "CommitDumper.h"
#include "GraphFileType.h"
#include "FileUtils.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"
#include "FilePageReader.h"
#include "GraphInfoLoader.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

DumpResult<void> GraphDumper::dump(const Graph& graph, const fs::Path& path) {
    Profile profile {"GraphDumper::dump"};

    auto lock = graph._versionController->lock();

    if (!path.exists()) {
        // Create directory
        if (auto res = path.mkdir(); !res) {
            return DumpError::result(DumpErrorType::CANNOT_MKDIR_GRAPH, res.error());
        }
    }

    // Dumping graph info
    {
        Profile profile {"GraphDumper::dump <graph info>"};
        const fs::Path infoPath = path / "info";

        if (infoPath.exists()) {
            auto reader = fs::FilePageReader::open(infoPath, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, reader.error());
            }

            const GraphInfoLoader loader {reader.value()};

            auto res = loader.isSameGraph(graph);
            if (!res) {
                return res.get_unexpected();
            }

            if (!res.value()) {
                return DumpError::result(DumpErrorType::GRAPH_DIR_ALREADY_EXISTS);
            }

            // Graph is already dumped, only dump what's missing
            return GraphDumper::dumpMissingCommits(graph, path);
        }

        auto writer = fs::FilePageWriter::open(infoPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, writer.error());
        }

        GraphInfoDumper dumper {writer.value()};

        if (auto res = dumper.dump(graph); !res) {
            return res;
        }
    }

    // Dump graph type
    {
        Profile profile {"GraphDumper::dump <graph type>"};
        const fs::Path graphTypePath = path / "type";

        if (graphTypePath.exists()) {
        }

        const auto typeTag = GraphFileTypeDescription::value(GraphFileType::BINARY);
        if (!FileUtils::writeFile(graphTypePath.get(), std::string(typeTag))) {
            return DumpError::result(DumpErrorType::CANNOT_WRITE_GRAPH_TYPE);
        }
    }

    // Dumping commits
    for (const auto& [i, commit] : graph._versionController->_commits | rv::enumerate) {
        const std::string fileName = fmt::format("commit-{}-{}", i, commit->hash().get());
        const fs::Path commitPath = path / fileName;

        if (auto res = CommitDumper::dump(*commit, commitPath); !res) {
            return res;
        }
    }

    return {};
}

DumpResult<void> GraphDumper::dumpMissingCommits(const Graph& graph, const fs::Path& path) {
    for (const auto& [i, commit] : graph._versionController->_commits | rv::enumerate) {
        const std::string fileName = fmt::format("commit-{}-{}", i, commit->hash().get());
        const fs::Path commitPath = path / fileName;

        if (commitPath.exists()) {
            continue;
        }

        if (auto res = CommitDumper::dump(*commit, commitPath); !res) {
            return res;
        }
    }

    return {};
}

