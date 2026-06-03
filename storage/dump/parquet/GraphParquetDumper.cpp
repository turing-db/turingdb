#include "GraphParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "CommitParquetDumper.h"
#include "GraphParquetLayout.h"

#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/VersionController.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

constexpr std::string_view GRAPH_ID_COLUMN = "graph_id";
constexpr std::string_view NAME_COLUMN = "name";
constexpr std::string_view COMMIT_HASH_COLUMN = "commit_hash";

void writeGraphInfo(const Graph& graph, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(GRAPH_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(NAME_COLUMN, ParquetColumnType::String);

    ParquetWriter writer(path, schema);

    const std::vector<int64_t> graphId {static_cast<int64_t>(graph.getID().get())};
    const std::vector<std::string_view> name {graph.getName()};

    writer.beginRowGroup(1);
    writer.writeInt64Column(0, graphId);
    writer.writeStringColumn(1, name);
    writer.finish();
}

void writeCommitLog(const std::vector<uint64_t>& hashesAscending, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(COMMIT_HASH_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);

    const size_t count = hashesAscending.size();
    if (count > 0) {
        std::vector<int64_t> hashes;
        hashes.reserve(count);
        for (const uint64_t hash : hashesAscending) {
            hashes.push_back(static_cast<int64_t>(hash));
        }

        writer.beginRowGroup(count);
        writer.writeInt64Column(0, hashes);
    }

    writer.finish();
}

}

void GraphParquetDumper::dump(const Graph& graph, const fs::Path& graphDir) {
    if (!graphDir.exists()) {
        if (const auto res = graphDir.mkdir(); !res) {
            throw FatalException("GraphParquetDumper: cannot create graph directory");
        }
    }

    writeGraphInfo(graph, graphParquetLayout::graphInfo(graphDir));

    const VersionController& controller = graph.getVersionController();
    const fs::Path partsDir = graphParquetLayout::dataPartsDir(graphDir);

    // Walk from head backwards, dumping each commit. Collected head-first; the commit log
    // is written oldest-first below.
    std::vector<uint64_t> hashesHeadFirst;
    for (const Commit* commit = controller.getCommitSafe(CommitHash::head());
         commit != nullptr;
         commit = commit->getPreviousCommit()) {
        const uint64_t hash = commit->hash().get();
        hashesHeadFirst.push_back(hash);

        const fs::Path commitDir = graphParquetLayout::commitDir(graphDir, hash);
        CommitParquetDumper::dump(*commit, commitDir, partsDir);
    }

    std::vector<uint64_t> hashesAscending(hashesHeadFirst.rbegin(), hashesHeadFirst.rend());
    writeCommitLog(hashesAscending, graphParquetLayout::commitLog(graphDir));
}
