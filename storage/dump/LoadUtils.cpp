#include "LoadUtils.h"

#include "AlignedBuffer.h"

using namespace db;

void LoadUtils::ensureIteratorReadPage(fs::AlignedBufferIterator& it) {
    // Check if we received a full page
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        const std::string info = fmt::format("Got {} bytes but needed {} bytes",
                                       it.remainingBytes(), DumpConfig::PAGE_SIZE);
        throw TuringException("Failed to read from file: iterator did not get full page "
                              "prior to loading nodes. "
                              + info);
    }
}

void LoadUtils::ensureLoadSpace(size_t requiredSpace,
                                fs::FilePageReader& rd,
                                fs::AlignedBufferIterator& it) {
    if (requiredSpace > DumpConfig::PAGE_SIZE) {
        const std::string info =
            fmt::format("Attempting to write {} bytes which exceedes page size of {}",
                        requiredSpace, DumpConfig::PAGE_SIZE);
        throw TuringException("Illegal read: " + info);
    }

    if (it.remainingBytes() < requiredSpace) {
        rd.nextPage();
        it = rd.begin();
        LoadUtils::ensureIteratorReadPage(it);
    }
}

void LoadUtils::newPage(fs::AlignedBufferIterator& it, fs::FilePageReader& rd) {
    rd.nextPage();
    it = rd.begin();
    ensureIteratorReadPage(it);
}
