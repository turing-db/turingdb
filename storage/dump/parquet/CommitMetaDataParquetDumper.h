#pragma once

#include "Path.h"

namespace db {

class Commit;

// Serializes a commit's metadata to commit-metadata.parquet in the commit directory: the
// ids of all dataparts in the commit's history as one data_part_id column (one row each),
// with num_nodes, num_edges and num_commit_dataparts in the file's key/value metadata.
// Mirrors the content of the binary "metadata" file. Throws on failure.
class CommitMetaDataParquetDumper {
public:
    static void dump(const Commit& commit, const fs::Path& commitDir);
};

}
