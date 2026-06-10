#include "CommitParquetDumper.h"

#include "GraphMetadataParquetDumper.h"
#include "CommitJournalParquetDumper.h"
#include "TombstonesParquetDumper.h"
#include "CommitMetaDataParquetDumper.h"
#include "DataPartParquetDumper.h"

#include "datapart/DataPart.h"
#include "datapart/DataPartSpan.h"
#include "versioning/Commit.h"
#include "versioning/CommitData.h"
#include "versioning/CommitHistory.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

void CommitParquetDumper::dump(const Commit& commit,
                               const fs::Path& commitDir,
                               const fs::Path& partsDir) {
    if (commitDir.exists()) {
        throw FatalException("CommitParquetDumper: commit directory already exists");
    }

    if (const auto res = commitDir.mkdir(); !res) {
        throw FatalException("CommitParquetDumper: cannot create commit directory");
    }

    // A shell commit (a lazily-loaded ancestor) has no CommitData in memory — only its
    // counters. There is nothing to dump but the commit metadata; on load only the shell
    // counts are read (its full data is never materialized), mirroring the binary path.
    if (!commit.hasData()) {
        CommitMetaDataParquetDumper::dump(commit, commitDir);
        return;
    }

    const CommitData& data = commit.data();

    GraphMetadataParquetDumper::dump(data.metadata(), commitDir);
    CommitJournalParquetDumper::dump(commit.history().journal(), commitDir);
    TombstonesParquetDumper::dump(data.tombstones(), commitDir);

    for (const auto& part : data.commitDataparts()) {
        const fs::Path partDir = partsDir / std::to_string(part->getID().get());

        // Each datapart belongs to exactly one commit, so collisions should not happen;
        // guard defensively to keep the dump idempotent across a shared parts directory.
        if (!partDir.exists()) {
            DataPartParquetDumper::dump(*part, partDir);
        }
    }

    CommitMetaDataParquetDumper::dump(commit, commitDir);
}
