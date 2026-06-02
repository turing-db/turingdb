#pragma once

#include "Path.h"

namespace db {

class CommitJournal;

// Serializes a CommitJournal's persisted write sets to two Parquet files in the commit
// directory: journal-nodes.parquet (node_id) and journal-edges.parquet (edge_id), one row
// per written id in write-set order. Split because the two sets have independent row
// counts. The property write sets are transient change-application state (not persisted by
// the binary format either, and untouched by clear/empty/finalise), so they are not
// written. Throws on failure.
class CommitJournalParquetDumper {
public:
    static void dump(const CommitJournal& journal, const fs::Path& commitDir);
};

}
