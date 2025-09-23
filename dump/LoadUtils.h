#pragma once

#include "AlignedBuffer.h"
#include "DumpResult.h"
#include "FilePageReader.h"
#include "DumpUtils.h"

namespace db {

class LoadUtils {
public:
    template <Dumpable T>
    [[nodiscard]] static DumpResult<void> loadVector(std::vector<T>& out,
                                                     size_t sz,
                                                     fs::FilePageReader& reader,
                                                     fs::AlignedBufferIterator& it);

    static void newPage(fs::AlignedBufferIterator& it, fs::FilePageReader& rd);

    static void ensureLoadSpace(size_t requiredSpace,
                         fs::FilePageReader& rd,
                         fs::AlignedBufferIterator& it);

    static void ensureIteratorReadPage(fs::AlignedBufferIterator& it);
};

template <Dumpable T>
DumpResult<void> LoadUtils::loadVector(std::vector<T>& out,
                                       size_t sz,
                                       fs::FilePageReader& reader,
                                       fs::AlignedBufferIterator& it) {
    using WorkingT = WorkingType<T>;

    out.clear();
    out.reserve(sz);

    size_t TSize = sizeof(WorkingT);
    if (TSize > DumpConfig::PAGE_SIZE) {
        std::string errMsg = fmt::format(
            "Attempted to load object {} with size {}, which exceeds page size of {}.",
            typeid(WorkingT).name(), TSize, DumpConfig::PAGE_SIZE);
        throw TuringException("Illegal write: " + errMsg);
    }

    const size_t remainingSpace = it.remainingBytes();
    size_t TsThisPage = remainingSpace / TSize; // Truncates
    TsThisPage = std::min(sz, TsThisPage);      // The amount that fit on this page

    // Read the number on this page
    for (size_t i = 0; i < TsThisPage; i++) {
        auto id = it.get<WorkingT>();
        out.emplace_back(id);
    }

    // If we all were on a single page: done
    if (sz == TsThisPage) {
        return {};
    }

    const size_t read = TsThisPage;
    const size_t remaining = sz - read;

    const size_t TsPerPage = DumpConfig::PAGE_SIZE / TSize;

    // Number of pages that were full
    const size_t fullPagesNeeded = remaining / TsPerPage;
    // Remainder on last page: doesn't fill the whole page
    const size_t leftOver = remaining % TsPerPage;

    // Get full pages
    for (size_t p = 0; p < fullPagesNeeded; p++) {
        LoadUtils::newPage(it, reader);
        for (size_t j = 0; j < TsPerPage; j++) {
            out.emplace_back(it.get<WorkingT>());
        }
        if (reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_STR_PROP_INDEXER,
                                     reader.error().value());
        }
    }

    if (leftOver != 0) {
        LoadUtils::newPage(it, reader);

        for (size_t j = 0; j < leftOver; j++) {
            out.emplace_back(it.get<WorkingT>());
        }
    }

    if (reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_STR_PROP_INDEXER,
                                     reader.error().value());
    }
    return {};
}

}
