#pragma once

#include <ranges>

#include "ID.h"
#include "Primitives.h"
#include "FilePageWriter.h"
#include "DumpResult.h"
#include "DumpConfig.h"
#include "TuringException.h"

namespace fs {

template <db::IntegralType T, int I>
void WriteToCurrentPage(fs::FilePageWriter& w, db::ID<T, I> id) {
    w.writeToCurrentPage(id.getValue());
}

}

namespace db {

// Non-specialised trait declares all types as not being dumpable IDs
template <typename, typename = void>
struct is_dumpable_id : std::false_type {};

// Specialism declares all ID types as dumpable IDs
template <IntegralType T, int Tag>
struct is_dumpable_id<ID<T, Tag>> : std::true_type {};

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

class DumpUtils {
public:
    /** * @brief Uses @param wr to write the range @param rg to page. May write over
     * multiple pages.
     * @detail sizeof(T) must be less than the page size. Empty space left per page is no
     * more than sizeof(T)
     */
    template <std::ranges::input_range Range>
        requires Dumpable<std::ranges::range_value_t<Range>>
    static DumpResult<void> dumpRange(const Range& rg, fs::FilePageWriter& wr);

    /**
     * @brief Checks if there is at least @param requiredSpace on the page @param wr is
     * currently writing to. If not, moves the writer to a new page.
     * @throws TuringException if @param requiredSpace exceeds @ref DumpConfig::PAGE_SIZE
     */
    static void ensureDumpSpace(size_t requiredSpace, fs::FilePageWriter& wr);
};

template <std::ranges::input_range Range>
    requires Dumpable<std::ranges::range_value_t<Range>>
DumpResult<void> DumpUtils::dumpRange(const Range& rg, fs::FilePageWriter& wr) {
    using T = std::ranges::range_value_t<Range>;
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
    const size_t countThisPage = remainingSpace / TSize;

    auto it = rg.begin();
    for (size_t j = 0; j < countThisPage && it != rg.end(); j++) {
        wr.writeToCurrentPage(*it);
        it++;
    }

    if (it == rg.end()) {
        return {};
    }

    const size_t read = countThisPage;
    const size_t remaining = rg.size() - read;

    const size_t countPerPage = DumpConfig::PAGE_SIZE / TSize;

    const size_t fullPagesNeeded = remaining / countPerPage;
    const size_t leftOver = remaining % countPerPage;

    for (size_t p = 0; p < fullPagesNeeded; p++) {
        wr.nextPage();
        for (size_t j = 0; j < countPerPage; j++) {
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

