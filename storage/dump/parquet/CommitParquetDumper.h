#pragma once

#include "Path.h"

namespace db {

class Commit;

// Serializes one commit to a Parquet commit directory by delegating to the commit-level
// adapters — the GraphMetadata schema maps, the commit journal, the tombstones, and the
// commit metadata — plus each of the commit's own dataparts (written into the shared
// dataparts directory, keyed by id). Mirrors the binary CommitDumper. Throws on failure.
class CommitParquetDumper {
public:
    static void dump(const Commit& commit, const fs::Path& commitDir, const fs::Path& partsDir);
};

}
