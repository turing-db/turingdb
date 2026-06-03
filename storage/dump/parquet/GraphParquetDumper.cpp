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

void writeGraphInfo(const Graph& graph, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(graphParquetLayout::GRAPH_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(graphParquetLayout::NAME_COLUMN, ParquetColumnType::String);

    ParquetWriter writer(path, schema);
    writer.setMetadata(graphParquetLayout::FORMAT_VERSION_KEY,
                       std::to_string(graphParquetLayout::FORMAT_VERSION));

    const std::vector<int64_t> graphId {static_cast<int64_t>(graph.getID().get())};
    const std::vector<std::string_view> name {graph.getName()};

    writer.beginRowGroup(1);
    writer.writeInt64Column(0, graphId);
    writer.writeStringColumn(1, name);
    writer.finish();
}

void writeCommitLog(const std::vector<uint64_t>& hashesAscending, const fs::Path& path) {
    ParquetWriteSchema schema;
    schema.addColumn(graphParquetLayout::COMMIT_HASH_COLUMN, ParquetColumnType::UInt64);

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
    // Snapshot under the version-controller lock, as the binary GraphDumper does, so a
    // concurrent commit cannot tear the head walk below.
    const auto lock = graph._versionController->lock();

    if (graphDir.exists()) {
        throw FatalException("GraphParquetDumper: graph directory already exists");
    }

    // Dump into a sibling temp directory and rename it over the final path on success, so
    // a partial dump (crash, disk full) is never mistaken for a complete one. A leftover
    // temp directory is a previous crashed dump — partial by construction, remove it.
    const fs::Path tempDir = graphParquetLayout::dumpTempDir(graphDir);
    if (tempDir.exists()) {
        if (const auto res = tempDir.rm(); !res) {
            throw FatalException("GraphParquetDumper: cannot remove stale dump temp directory");
        }
    }

    if (const auto res = tempDir.mkdir(); !res) {
        throw FatalException("GraphParquetDumper: cannot create dump temp directory");
    }

    writeGraphInfo(graph, graphParquetLayout::graphInfo(tempDir));

    const fs::Path partsDir = graphParquetLayout::dataPartsDir(tempDir);

    // Walk from head backwards, dumping each commit. Collected head-first; the commit log
    // is written oldest-first below. The head is read directly, as the binary GraphDumper
    // does — getCommitSafe would re-take the version-controller mutex held above.
    std::vector<uint64_t> hashesHeadFirst;
    for (const Commit* commit = graph._versionController->_head.load();
         commit != nullptr;
         commit = commit->getPreviousCommit()) {
        const uint64_t hash = commit->hash().get();
        hashesHeadFirst.push_back(hash);

        const fs::Path commitDir = graphParquetLayout::commitDir(tempDir, hash);
        CommitParquetDumper::dump(*commit, commitDir, partsDir);
    }

    std::vector<uint64_t> hashesAscending(hashesHeadFirst.rbegin(), hashesHeadFirst.rend());
    writeCommitLog(hashesAscending, graphParquetLayout::commitLog(tempDir));

    if (const auto res = tempDir.rename(graphDir); !res) {
        throw FatalException("GraphParquetDumper: cannot rename dump temp directory into place");
    }
}
