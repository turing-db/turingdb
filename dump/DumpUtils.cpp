#include "DumpUtils.h"

#include "spdlog/spdlog.h"

#include "DumpConfig.h"
#include "TuringException.h"

using namespace db;

void DumpUtils::ensureDumpSpace(size_t requiredSpace, fs::FilePageWriter& wr) {
    if (requiredSpace > DumpConfig::PAGE_SIZE) {
        spdlog::error("Attempting to write {} bytes which exceedes page size of {}",
                     requiredSpace, DumpConfig::PAGE_SIZE);
        throw TuringException("Illegal write.");
    }
    if (wr.buffer().avail() < requiredSpace) {
        wr.nextPage();
    }
}
