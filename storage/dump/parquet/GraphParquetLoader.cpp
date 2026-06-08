#include "GraphParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "CommitParquetLoader.h"
#include "GraphParquetLayout.h"
#include "ParquetMetadataParsing.h"

#include "Graph.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/GraphID.h"
#include "versioning/VersionController.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

// graph-info.parquet's single row: the graph id and name.
struct GraphInfoData {
    uint64_t _graphId {0};
    std::string _name;
};

// graph-info.parquet: one row, graph_id (INT64) + name (BYTE_ARRAY). Also carries the
// dump-wide format version in its key/value metadata; it is the first file the loader
// reads, so an incompatible dump is rejected before anything else is interpreted. Fills
// the caller-owned GraphInfoData and tracks the row count for the caller to validate.
class GraphInfoVisitor : public ParquetSaxVisitor {
public:
    explicit GraphInfoVisitor(GraphInfoData& data)
        : _data(data) {
    }

    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();

        bool hasVersion = false;
        uint64_t version = 0;
        if (keyValueMetadata) {
            for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
                if (keyValueMetadata->key(i) == graphParquetLayout::FORMAT_VERSION_KEY) {
                    version = parseMetadataUint64(keyValueMetadata->key(i),
                                                  keyValueMetadata->value(i));
                    hasVersion = true;
                }
            }
        }

        if (!hasVersion) {
            throw FatalException("GraphParquetLoader: dump has no format version"
                                 " — not a TuringDB Parquet graph dump?");
        }

        if (version != graphParquetLayout::FORMAT_VERSION) {
            throw FatalException(fmt::format(
                "GraphParquetLoader: unsupported dump format version {} (supported: {})",
                version, graphParquetLayout::FORMAT_VERSION));
        }

        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex == 0 && !values.empty()) {
            _data._graphId = static_cast<uint64_t>(values[0]);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex, std::span<const parquet::ByteArray> values) override {
        if (columnIndex == 1 && !values.empty()) {
            const parquet::ByteArray& byteArray = values[0];
            _data._name.assign(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }

    bool onChunkEnd(size_t rowGroupIndex, size_t firstRowInRowGroup, size_t rows) override {
        _rows += rows;
        return true;
    }

    size_t getRowCount() const { return _rows; }

private:
    GraphInfoData& _data;
    size_t _rows {0};
};

// commit-log.parquet: commit_hash (INT64), one row per commit, oldest first. Collected
// into the caller-owned vector.
class CommitLogVisitor : public ParquetSaxVisitor {
public:
    explicit CommitLogVisitor(std::vector<uint64_t>& hashes)
        : _hashes(hashes) {
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        for (const int64_t value : values) {
            _hashes.push_back(static_cast<uint64_t>(value));
        }
        return true;
    }

private:
    std::vector<uint64_t>& _hashes;
};

}

void GraphParquetLoader::load(Graph* graph, const fs::Path& graphDir) {
    // Loading replaces the graph's version controller wholesale; refuse a target that
    // already carries commits beyond the initial empty one rather than dropping them.
    const bool graphHasHistory = graph->_versionController != nullptr
                                 && graph->_versionController->getNumCommits() > 1;
    if (graphHasHistory) {
        throw FatalException("GraphParquetLoader: target graph already has commits");
    }

    GraphInfoData info;
    {
        GraphInfoVisitor visitor(info);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(graphParquetLayout::GRAPH_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(graphParquetLayout::NAME_COLUMN, ParquetColumnType::String);

        ParquetReader reader(graphParquetLayout::graphInfo(graphDir), visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }

        if (visitor.getRowCount() != 1) {
            throw FatalException(fmt::format(
                "GraphParquetLoader: graph-info must have exactly one row, found {}",
                visitor.getRowCount()));
        }
    }

    graph->_graphID = GraphID {info._graphId};
    graph->_graphName = info._name;

    graph->_versionController = std::make_unique<VersionController>(graph);
    VersionController* controller = graph->_versionController.get();

    std::vector<uint64_t> commitHashes;
    {
        CommitLogVisitor visitor(commitHashes);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(graphParquetLayout::COMMIT_HASH_COLUMN, ParquetColumnType::UInt64);

        ParquetReader reader(graphParquetLayout::commitLog(graphDir), visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    const Commit* prevCommit = nullptr;
    for (const uint64_t hash : commitHashes) {
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
