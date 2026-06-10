#pragma once

#include "Path.h"

namespace db {

class CommitJournal;

// Rebuilds a CommitJournal's node and edge write sets from the files written by
// CommitJournalParquetDumper, read straight into the raw write-set vectors in dumped order
// (already sorted/unique from finalise — nothing re-sorted). The output journal is expected
// to be empty. Friend of CommitJournal to reach the raw write-set vectors. Throws on
// failure (I/O, decode).
class CommitJournalParquetLoader {
public:
    static void load(const fs::Path& commitDir, CommitJournal& out);
};

}
