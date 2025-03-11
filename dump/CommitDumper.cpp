#include "CommitDumper.h"

#include <range/v3/view/enumerate.hpp>

#include "versioning/Commit.h"
#include "DataPartDumper.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

DumpResult<void> CommitDumper::dump(const Commit& commit, const fs::Path& path) {
    if (path.exists()) {
        return DumpError::result(DumpErrorType::COMMIT_ALREADY_EXISTS);
    }

    if (auto res = path.mkdir(); !res) {
        return DumpError::result(DumpErrorType::CANNOT_MKDIR_COMMIT, res.get_unexpected().error());
    }

    for (const auto& [i, part] : commit.data().commitDataparts() | rv::enumerate) {
        const fs::Path partPath = path / "datapart-" + std::to_string(i);

        if (auto res = DataPartDumper::dump(*part, partPath); !res) {
            return res;
        }
    }

    return {};
}
