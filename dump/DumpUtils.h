#pragma once

#include "Primitives.h"
#include "TuringException.h"

#include "DumpConfig.h"
#include "DumpResult.h"
#include "FilePageWriter.h"

// Non-specialised trait declares all types as not being dumpable IDs
template <typename, typename = void>
struct is_dumpable_id : std::false_type {};

// Specialised trait declares all ID types (determine if T::Type) exists as dumpable IDs
template <typename T>
struct is_dumpable_id<T, std::void_t<typename T::Type>> : std::true_type {};

// Concept using the above traits
template <typename T>
concept DumpableID = is_dumpable_id<T>::value;

// Concept to constrain a trivial type to be dumped
template <typename T>
concept TriviallyDumpable = fs::TrivialPrimitive<T>;

// Dumpable concept: either an ID or a trivial type
template <typename T>
concept Dumpable = DumpableID<T> || TriviallyDumpable<T>;

// Traits to get the underlying type of an ID to dump/load it
template <typename T, bool IsID>
struct WorkingTypeHelper;

// If an ID type: the underlying type to dump is the ::Type property of the ID
template <typename T>
struct WorkingTypeHelper<T, true> { using type = typename T::Type; };

// If not an ID type: use the actual type as the type to dump
template <typename T>
struct WorkingTypeHelper<T, false> { using type = T; };

// Helper for Dump/Load: gets the underlying type of an ID or just the trivial type
template <typename T>
using WorkingType = typename WorkingTypeHelper<T, DumpableID<T>>::type;

namespace db {

class DumpUtils {
public:
    template<Dumpable T>
    static DumpResult<void> dumpVector(const std::vector<T>& vec, fs::FilePageWriter& wr);

    static void ensureDumpSpace(size_t requiredSpace, fs::FilePageWriter& wr);
};

template<Dumpable T>
DumpResult<void> DumpUtils::dumpVector(const std::vector<T>& vec, fs::FilePageWriter& wr) {
    using WorkingT = WorkingType<T>;

    const size_t TSize = sizeof(WorkingT);
    if (TSize > DumpConfig::PAGE_SIZE) {
        std::string errMsg = fmt::format(
            "Attempted to dump object {} with size {}, which exceeds page size of {}.",
            typeid(WorkingT).name(), TSize, DumpConfig::PAGE_SIZE);
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

