#pragma once

#include "Primitives.h"
#include "TuringException.h"

#include "DumpConfig.h"
#include "DumpResult.h"
#include "FilePageWriter.h"


namespace db {

class DumpUtils {
public:
    /**
     * @brief Uses @param wr to write @param vec to page. May write over multiple pages.
     * @detail sizeof(T) must be less than the page size. Empty space left per page is no
     * more than sizeof(T)
     */
    template<fs::Dumpable T>
    static DumpResult<void> dumpVector(const std::vector<T>& vec, fs::FilePageWriter& wr);

    /**
     * @brief Checks if there is at least @param requiredSpace on the page @param wr is
     * currently writing to. If not, moves the writer to a new page.
     * @throws TuringException if @param requiredSpace exceeds @ref DumpConfig::PAGE_SIZE
     */
    static void ensureDumpSpace(size_t requiredSpace, fs::FilePageWriter& wr);
};

template<fs::Dumpable T>
DumpResult<void> DumpUtils::dumpVector(const std::vector<T>& vec, fs::FilePageWriter& wr) {
    using WorkingT = fs::WorkingType<T>;

    const size_t TSize = sizeof(WorkingT);
    if (TSize > DumpConfig::PAGE_SIZE) {
        std::string errMsg = fmt::format(
            "Attempted to dump object {} with size {}, which exceeds page size of {}.",
            typeid(WorkingT).name(), TSize, DumpConfig::PAGE_SIZE);
        throw TuringException("Illegal write: " + errMsg);
    }

    // Write as many as we can on this page
    const size_t remainingSpace = wr.buffer().avail();
    const size_t TsThisPage = remainingSpace / TSize;

    auto it = vec.cbegin();
    for (size_t j = 0; j < TsThisPage && it != vec.cend(); j++) {
        wr.writeToCurrentPage(*it);
        it++;
    }

    if (it == vec.cend()) {
        return {};
    }

    const size_t read = TsThisPage;
    const size_t remaining = vec.size() - read;

    const size_t TsPerPage = DumpConfig::PAGE_SIZE / TSize;

    const size_t fullPagesNeeded = remaining / TsPerPage;
    const size_t leftOver = remaining % TsPerPage;

    for (size_t p = 0; p < fullPagesNeeded; p++) {
        wr.nextPage();
        for (size_t j = 0; j < TsPerPage; j++) {
            wr.writeToCurrentPage(*it);
            it++;
        }
    }

    if (leftOver != 0) {
        wr.nextPage();
        for (size_t j = 0; j < leftOver; j++) {
            wr.writeToCurrentPage(*it);
            it++;
        }
    }

    return {};
}

}

