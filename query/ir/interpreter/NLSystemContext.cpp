#include "NLSystemContext.h"

#include "Path.h"

#include "IRException.h"

using namespace db;

NLSystemContext::NLSystemContext() {
}

NLSystemContext::~NLSystemContext() {
}

void NLSystemContext::resolveInDataDir(fs::Path& resolved,
                                       const fs::Path& dataDir,
                                       std::string_view path) {
    resolved = dataDir / path;

    if (!resolved.isSubDirectory(dataDir)) {
        throw IRException(fmt::format("Invalid file path '{}': it must be relative to '{}'",
                                      path,
                                      dataDir.get()));
    }
}
