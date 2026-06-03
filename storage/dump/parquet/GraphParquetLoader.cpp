#include "GraphParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <vector>

#include <parquet/types.h>

#include "ParquetReader.h"

#include "CommitParquetLoader.h"
#include "GraphParquetLayout.h"

#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/GraphID.h"
#include "versioning/VersionController.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

// graph-info.parquet: one row, graph_id (INT64) + name (BYTE_ARRAY).
class GraphInfoVisitor : public ParquetSaxVisitor {
public:
    uint64_t _graphId {0};
    std::string _name;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex == 0 && !values.empty()) {
            _graphId = static_cast<uint64_t>(values[0]);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex, std::span<const parquet::ByteArray> values) override {
        if (columnIndex == 1 && !values.empty()) {
            const parquet::ByteArray& byteArray = values[0];
            _name.assign(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }
};

// commit-log.parquet: commit_hash (INT64), one row per commit, oldest first.
class CommitLogVisitor : public ParquetSaxVisitor {
public:
    std::vector<uint64_t> _hashes;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t value : values) {
            _hashes.push_back(static_cast<uint64_t>(value));
        }
        return true;
    }
};

}

void GraphParquetLoader::load(Graph* graph, const fs::Path& graphDir) {
    GraphInfoVisitor info;
    {
        ParquetReader reader(graphParquetLayout::graphInfo(graphDir), info);
        while (reader.nextChunk()) {
        }
    }

    graph->_graphID = GraphID {info._graphId};
    graph->_graphName = info._name;

    graph->_versionController = std::make_unique<VersionController>(graph);
    VersionController* controller = graph->_versionController.get();

    CommitLogVisitor commitLog;
    {
        ParquetReader reader(graphParquetLayout::commitLog(graphDir), commitLog);
        while (reader.nextChunk()) {
        }
    }

    const Commit* prevCommit = nullptr;
    for (const uint64_t hash : commitLog._hashes) {
        const fs::Path commitDir = graphParquetLayout::commitDir(graphDir, hash);

        std::unique_ptr<Commit> commit =
            CommitParquetLoader::load(controller, CommitHash {hash}, commitDir, prevCommit);

        prevCommit = commit.get();
        controller->addCommit(std::move(commit));
    }

    Commit* headCommit = controller->_head.load();
    if (!headCommit) {
        throw FatalException("GraphParquetLoader: graph has no commits");
    }

    const fs::Path headCommitDir = graphParquetLayout::commitDir(graphDir, headCommit->hash().get());
    const fs::Path partsDir = graphParquetLayout::dataPartsDir(graphDir);

    CommitParquetLoader::loadData(headCommitDir, partsDir, controller, headCommit);
}
