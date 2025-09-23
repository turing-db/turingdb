#pragma once

#include "Primitives.h"
#include "TuringException.h"
#include "spdlog/spdlog.h"

#include "DumpConfig.h"
#include "DumpResult.h"
#include "FilePageWriter.h"

template <typename T>
concept Dumpable = requires {
    sizeof(T);
    typename std::vector<T>; // Must be instantiable in a vector
    fs::TrivialPrimitive<T>; // Must be writable
};

namespace db {

class DumpUtils {
public:
    template<Dumpable T>
    static DumpResult<void> dumpVector(const std::vector<T>& vec, fs::FilePageWriter& wr);

    static void ensureDumpSpace(size_t requiredSpace, fs::FilePageWriter& wr);
};

template<Dumpable T>
DumpResult<void> DumpUtils::dumpVector(const std::vector<T>& vec, fs::FilePageWriter& wr) {
    const size_t TSize = sizeof(T);
    if (TSize > DumpConfig::PAGE_SIZE) {
        std::string errMsg = fmt::format(
            "Attempted to dump object {} with size {}, which exceeds page size of {}.",
            typeid(T).name(), TSize, DumpConfig::PAGE_SIZE);
        throw TuringException("Illegal write: " + errMsg);
    }

    // Write as many as we can on this page
    const size_t remainingSpace = wr.buffer().avail();
    const size_t TsThisPage = remainingSpace / TSize; // Truncates

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

