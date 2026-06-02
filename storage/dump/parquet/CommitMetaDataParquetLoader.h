#pragma once

#include <stddef.h>

#include <vector>

#include "versioning/DataPartID.h"
#include "Path.h"

namespace db {

// Source-independent decode of a commit's metadata. The future CommitDumper/CommitLoader
// wiring maps this onto the Commit (num_nodes/num_edges/num_commit_dataparts) and drives
// datapart loading from the id list.
struct CommitParquetMetaData {
    size_t _numNodes {0};
    size_t _numEdges {0};
    size_t _numCommitDataParts {0};
    std::vector<DataPartID> _allDatapartIds;
};

// Reads commit-metadata.parquet written by CommitMetaDataParquetDumper into the output
// struct: the scalars from the file's key/value metadata, the datapart ids from the
// data_part_id column. Throws on failure (missing metadata, I/O, decode).
class CommitMetaDataParquetLoader {
public:
    static void load(const fs::Path& commitDir, CommitParquetMetaData& out);
};

}
