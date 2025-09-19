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

    static void ensureSpace(size_t requiredSpace, fs::FilePageWriter& wr);
};

template<Dumpable T>
DumpResult<void> DumpUtils::dumpVector(const std::vector<T>& vec, fs::FilePageWriter& wr) {
    size_t TSize = sizeof(T);

    if (TSize > DumpConfig::PAGE_SIZE) {
        std::string errMsg = fmt::format(
            "Attempted to dump object {} with size {}, which exceeds page size of {}.",
            typeid(T).name(), TSize, DumpConfig::PAGE_SIZE);
        throw TuringException("Illegal write: " + errMsg);
    }

    // Write as many as we can on this page
    size_t remainingSpace = wr.buffer().avail();
    size_t TsThisPage = remainingSpace / TSize;

    auto it = vec.cbegin();
    while (TsThisPage-- && it != vec.cend()) {
        wr.writeToCurrentPage(*it++);
    }
    wr.nextPage();

    if (it == vec.cend()) {
        return {};
    }

    size_t TsPerPage = DumpConfig::PAGE_SIZE / TSize;

    while (it != vec.end()) {
        for (size_t i {0}; i < TsPerPage && it != vec.cend(); i++) {
            wr.writeToCurrentPage(*it);
            it++;
        }
        wr.nextPage();
    }

    return {};
}

}

