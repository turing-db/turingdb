#pragma once

#include <stddef.h>

#include <vector>

#include "versioning/DataPartID.h"
#include "Path.h"

namespace db {

class CommitMetaDataParquetLoader;

// Source-independent decode of a commit's metadata. CommitParquetLoader maps this onto
// the Commit (num_nodes/num_edges/num_commit_dataparts) and drives datapart loading
// from the id list.
class CommitParquetMetaData {
public:
    using DataPartIDs = std::vector<DataPartID>;

    [[nodiscard]] size_t getNumNodes() const { return _numNodes; }
    [[nodiscard]] size_t getNumEdges() const { return _numEdges; }
    [[nodiscard]] size_t getNumCommitDataParts() const { return _numCommitDataParts; }
    [[nodiscard]] const DataPartIDs& getAllDatapartIds() const { return _allDatapartIds; }

private:
    friend CommitMetaDataParquetLoader;

    size_t _numNodes {0};
    size_t _numEdges {0};
    size_t _numCommitDataParts {0};
    DataPartIDs _allDatapartIds;
};

// Reads commit-metadata.parquet written by CommitMetaDataParquetDumper into the output
// object: the scalars from the file's key/value metadata, the datapart ids from the
// data_part_id column. Throws on failure (missing metadata, I/O, decode).
class CommitMetaDataParquetLoader {
public:
    static void load(const fs::Path& commitDir, CommitParquetMetaData& out);
};

}
