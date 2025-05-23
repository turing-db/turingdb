#include "GraphDumper.h"

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "GraphInfoDumper.h"
#include "CommitDumper.h"
#include "GraphFileType.h"
#include "FileUtils.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

DumpResult<void> GraphDumper::dump(const Graph& graph, const fs::Path& path) {
    Profile profile {"GraphDumper::dump"};
    if (path.exists()) {
        return DumpError::result(DumpErrorType::GRAPH_DIR_ALREADY_EXISTS);
    }

    // Create directory
    if (auto res = path.mkdir(); !res) {
        return DumpError::result(DumpErrorType::CANNOT_MKDIR_GRAPH, res.error());
    }

    graph._versionController->lock();

    // Dump graph type
    {
        Profile profile {"GraphDumper::dump <graph type>"};
        const fs::Path graphTypePath = path / "type";
        const auto typeTag = GraphFileTypeDescription::value(GraphFileType::BINARY);
        if (!FileUtils::writeFile(graphTypePath.get(), std::string(typeTag))) {
            return DumpError::result(DumpErrorType::CANNOT_WRITE_GRAPH_TYPE);
        }
    }

    // Dumping graph info
    {
        Profile profile {"GraphDumper::dump <graph info>"};
        const fs::Path infoPath = path / "info";

        auto writer = fs::FilePageWriter::open(infoPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_GRAPH_INFO, writer.error());
        }

        GraphInfoDumper dumper {writer.value()};

        if (auto res = dumper.dump(graph); !res) {
            return res;
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

    graph._versionController->unlock();

    return {};
}

