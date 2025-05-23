#include "CommitDumper.h"

#include <range/v3/view/enumerate.hpp>

#include "LabelMapDumper.h"
#include "LabelSetMapDumper.h"
#include "EdgeTypeMapDumper.h"
#include "Profiler.h"
#include "PropertyTypeMapDumper.h"
#include "versioning/Commit.h"
#include "DataPartDumper.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

DumpResult<void> CommitDumper::dump(const Commit& commit, const fs::Path& path) {
    Profile profile {"CommitDumper::dump"};
    if (path.exists()) {
        return DumpError::result(DumpErrorType::COMMIT_ALREADY_EXISTS);
    }

    if (auto res = path.mkdir(); !res) {
        return DumpError::result(DumpErrorType::CANNOT_MKDIR_COMMIT, res.get_unexpected().error());
    }

    const auto& metadata = commit.data().metadata();

    // Dumping labels
    {
        Profile profile {"CommitDumper::dump <labels>"};
        const fs::Path labelsPath = path / "labels";

        auto writer = fs::FilePageWriter::open(labelsPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELS, writer.error());
        }

        LabelMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata.labels()); !res) {
            return res;
        }
    }

    // Dumping edge types
    {
        Profile profile {"CommitDumper::dump <edge types>"};
        const fs::Path edgetypesPath = path / "edge-types";

        auto writer = fs::FilePageWriter::open(edgetypesPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_EDGE_TYPES, writer.error());
        }

        EdgeTypeMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata.edgeTypes()); !res) {
            return res;
        }
    }

    // Dumping property types
    {
        Profile profile {"CommitDumper::dump <property types>"};
        const fs::Path proptypesPath = path / "property-types";

        auto writer = fs::FilePageWriter::open(proptypesPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, writer.error());
        }

        PropertyTypeMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata.propTypes()); !res) {
            return res;
        }
    }

    // Dumping labelsets
    {
        Profile profile {"CommitDumper::dump <labelsets>"};
        const fs::Path labelsetsPath = path / "labelsets";

        auto writer = fs::FilePageWriter::open(labelsetsPath, DumpConfig::PAGE_SIZE);
        if (!writer) {
            return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELSETS, writer.error());
        }

        LabelSetMapDumper dumper {writer.value()};

        if (auto res = dumper.dump(metadata.labelsets()); !res) {
            return res;
        }
    }


    for (const auto& [i, part] : commit.data().commitDataparts() | rv::enumerate) {
        const fs::Path partPath = path / "datapart-" + std::to_string(i);

        if (auto res = DataPartDumper::dump(*part, partPath); !res) {
            return res;
        }
    }

    return {};
}
