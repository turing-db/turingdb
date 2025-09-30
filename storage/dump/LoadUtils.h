#pragma once

#include "AlignedBuffer.h"
#include "DumpResult.h"
#include "FilePageReader.h"
#include "DumpConfig.h"
#include "TuringException.h"
#include "DumpUtils.h"

namespace db {

class LoadUtils {
public:
    /**
     * @brief Reads from the current position of @param it, attempting to read @param sz
     * elements of type @ref T. If the size of the read exceeds or runs over a page size,
     * reads from the next page of @param reader, updating @param it in place.
     */
    template <Dumpable T>
    [[nodiscard]] static DumpResult<void> loadVector(std::vector<T>& out,
                                                     size_t sz,
                                                     fs::FilePageReader& reader,
                                                     fs::AlignedBufferIterator& it);

    /**
    * @brief Moves @param rd to the next page, upating @ref it to the start of this new page.
    * @detail Checks that upon turning the page, the iterator recieved the expected PAGE_SIZE.
    */
    static void newPage(fs::AlignedBufferIterator& it, fs::FilePageReader& rd);

    /**
     * @brief Checks if there is at least @param requiredSpace on the page @param it is
     * curently reading from. If not, moves @param rd to the next page, and updates @param
     * it to the start of this new page.
     * @throws TuringException if @param requiredSpace exceeds @ref DumpConfig::PAGE_SIZE
     */
    static void ensureLoadSpace(size_t requiredSpace,
                         fs::FilePageReader& rd,
                         fs::AlignedBufferIterator& it);

    /**
     * @brief Checks if the number of bytes remaining for @param it is equal to @ref
     * DumpConfig::PAGE_SIZE
     * @warn Should only be called prior to any calls to @ref get() on @param it, such as
     * after calling @ref FilePageReader::nextPage.
     */
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
    size_t countThisPage = remainingSpace / TSize; // Truncates
    countThisPage = std::min(sz, countThisPage);      // The amount that fit on this page

    // Read the number on this page
    for (size_t i = 0; i < countThisPage; i++) {
        auto id = it.get<WorkingT>();
        out.emplace_back(id);
    }

    // If we all were on a single page: done
    if (sz == countThisPage) {
        return {};
    }

    const size_t read = countThisPage;
    const size_t remaining = sz - read;

    const size_t countPerPage = DumpConfig::PAGE_SIZE / TSize;

    // Number of pages that were full
    const size_t fullPagesNeeded = remaining / countPerPage;
    // Remainder on last page: doesn't fill the whole page
    const size_t leftOver = remaining % countPerPage;

    // Get full pages
    for (size_t p = 0; p < fullPagesNeeded; p++) {
        LoadUtils::newPage(it, reader);
        for (size_t j = 0; j < countPerPage; j++) {
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
