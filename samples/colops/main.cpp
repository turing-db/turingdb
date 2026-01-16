#include <concepts>
#include <iostream>
#include <memory>

#include "PropertyOperators.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

using namespace db;

using ColumnInts = ColumnVector<types::Int64::Primitive>;

static_assert(std::same_as<contained_type<ColumnInts*>::type, types::Int64::Primitive>);

auto
main() -> int {
    ColumnInts veca {1, 2, 3};
    ColumnInts vecb {9, 8, 7};
    ColumnInts res {};

    // properties::exec<properties::Add>(&res, &veca, &vecb);
}
