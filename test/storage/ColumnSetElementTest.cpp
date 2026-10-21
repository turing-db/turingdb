#include <gtest/gtest.h>

#include <concepts>
#include <tuple>

#include "columns/ColumnOperatorDispatcher.h"
#include "metadata/LabelSet.h"

using namespace db;

namespace {

template <typename Tuple>
struct EverySetElementIsComparable;

template <typename... Types>
struct EverySetElementIsComparable<std::tuple<Types...>> {
    static constexpr bool value = ((!ColumnSupportsInternal<ColumnSet, Types> || std::equality_comparable<Types>) && ...);
};

}

// libc++ compares keys when it copy-assigns an unordered_set and libstdc++ does not, so a
// set element with a hash but no == builds on Linux and fails on macOS.
TEST(ColumnSetElementTest, everySupportedElementIsEqualityComparable) {
    static_assert(EverySetElementIsComparable<InternalKind::Types::Tuple>::value);
}

TEST(ColumnSetElementTest, listsAndMapsAreSetElementsWithoutTheirOrderHeader) {
    static_assert(ColumnSupportsInternal<ColumnSet, ListView>);
    static_assert(ColumnSupportsInternal<ColumnSet, MapView>);
    static_assert(ColumnSupportsInternal<ColumnSet, std::optional<ListView>>);
    static_assert(ColumnSupportsInternal<ColumnSet, std::optional<MapView>>);
}
