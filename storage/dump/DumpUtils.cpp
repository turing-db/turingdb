#include "DumpUtils.h"

#include <spdlog/fmt/bundled/format.h>

#include "DumpConfig.h"
#include "TuringException.h"

using namespace db;

void DumpUtils::ensureDumpSpace(size_t requiredSpace, fs::FilePageWriter& wr) {
    if (requiredSpace > DumpConfig::PAGE_SIZE) {
        const std::string info =
            fmt::format("Attempting to write {} bytes which exceedes page size of {}",
                        requiredSpace, DumpConfig::PAGE_SIZE);
        throw TuringException("Illegal write: " + info);
    }
    if (wr.buffer().avail() < requiredSpace) {
        wr.nextPage();
    }
}
