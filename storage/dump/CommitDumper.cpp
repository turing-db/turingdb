#include "CommitDumper.h"

#include <range/v3/view/enumerate.hpp>

#include "FilePageWriter.h"
#include "DataPartDumper.h"
#include "LabelMapDumper.h"
#include "LabelSetMapDumper.h"
#include "EdgeTypeMapDumper.h"
#include "Path.h"
#include "PropertyTypeMapDumper.h"
#include "CommitJournalDumper.h"
#include "DumpConfig.h"

#include "dump/TombstonesDumper.h"
#include "versioning/Commit.h"
#include "versioning/CommitJournal.h"

#include "Profiler.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

DumpResult<void> CommitDumper::dump(const Commit& commit, const fs::Path& commitDir) {
    Profile profile("CommitDumper::dump");

    if (commitDir.exists()) {
        return DumpError::result(DumpErrorType::COMMIT_ALREADY_EXISTS);
    }

    if (auto res = commitDir.mkdir(); !res) {
        return DumpError::result(DumpErrorType::CANNOT_MKDIR_COMMIT, res.get_unexpected().error());
    }

    const auto& metadata = commit.data().metadata();

    // Dumping labels
    {
        Profile profile("CommitDumper::dump <labels>");
        const fs::Path labelsFile = commitDir / "labels";

        auto writer = fs::FilePageWriter::open(labelsFile, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELS, writer.error());
        }

        LabelMapDumper dumper(writer.value());

        if (auto res = dumper.dump(metadata.labels()); !res) {
            return res;
        }
    }

    // Dumping edge types
    {
        Profile profile("CommitDumper::dump <edge types>");
        const fs::Path edgetypesFile = commitDir / "edge-types";

        auto writer = fs::FilePageWriter::open(edgetypesFile, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_EDGE_TYPES, writer.error());
        }

        EdgeTypeMapDumper dumper(writer.value());

        if (auto res = dumper.dump(metadata.edgeTypes()); !res) {
            return res;
        }
    }

    // Dumping property types
    {
        Profile profile("CommitDumper::dump <property types>");
        const fs::Path proptypesFile = commitDir / "property-types";

        auto writer = fs::FilePageWriter::open(proptypesFile, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, writer.error());
        }

        PropertyTypeMapDumper dumper(writer.value());

        if (auto res = dumper.dump(metadata.propTypes()); !res) {
            return res;
        }
    }

    // Dumping labelsets
    {
        Profile profile("CommitDumper::dump <labelsets>");
        const fs::Path labelsetsFile = commitDir / "labelsets";

        auto writer = fs::FilePageWriter::open(labelsetsFile, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELSETS, writer.error());
        }

        LabelSetMapDumper dumper(writer.value());

        if (auto res = dumper.dump(metadata.labelsets()); !res) {
            return res;
        }
    }

    // Dumping Journal
    {
        Profile profile("CommitDumper::dump <journal>");
        const fs::Path journalFile = commitDir / "journal";

        auto writerRes = fs::FilePageWriter::open(journalFile, DumpConfig::PAGE_SIZE);
        if (!writerRes) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_JOURNAL, writerRes.error());
        }

        const CommitJournal& journal = commit.history().journal();

        CommitJournalDumper dumper(writerRes.value());
        if (auto res = dumper.dump(journal); !res) {
            return res;
        }
    }

    // Dumping tombstones
    {
        Profile profile("CommitDumper::dump <tombstones>");
        const fs::Path tombstonesFile = commitDir / "tombstones";

        auto writerRes = fs::FilePageWriter::open(tombstonesFile, DumpConfig::PAGE_SIZE);
        if (!writerRes) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_TOMBSTONES, writerRes.error());
        }

        const Tombstones& tombstones = commit.data().tombstones();

        TombstonesDumper dumper(writerRes.value());
        if (auto res = dumper.dump(tombstones); !res) {
            return res;
        }
    }

    // Dumping Merge File
    // Existence Of File Confirms If We Have A Merge Commit
    {
        Profile profile("CommitDumper::dump <merge>");
        if (commit.isMergeCommit()) {
            const fs::Path mergeFile = commitDir / "merge";
            auto writerRes = fs::FilePageWriter::open(mergeFile, DumpConfig::PAGE_SIZE);
            if (!writerRes) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_MERGE, writerRes.error());
            }
        }
    }

    for (const auto& [i, part] : commit.data().commitDataparts() | rv::enumerate) {
        const fs::Path partDir = commitDir / "datapart-" + std::to_string(i);

        if (auto res = DataPartDumper::dump(*part, partDir); !res) {
            return res;
        }
    }

    return {};
}
