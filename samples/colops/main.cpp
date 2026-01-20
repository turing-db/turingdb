#include <concepts>
#include <ios>
#include <iostream>
#include <optional>

#include "ColumnCombinations.h"
#include "LocalMemory.h"
#include "PropertyOperators.h"
#include "columns/ColumnOptMask.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

using namespace db;

using ColumnInts = ColumnVector<types::Int64::Primitive>;
using ColumnBools = ColumnVector<bool>;
using MaybeNodeIDs = ColumnOptVector<NodeID>;
using MaybeBools = ColumnOptVector<bool>;

static_assert(std::same_as<contained_type<ColumnInts*>::type, types::Int64::Primitive>);

/*

Column operators:

switch (op) {
    case (OP_ADD): exec<Add>(res, lhs, rhs);
    case (OP_SUB): exec<Sub>(res, lhs, rhs);
}

*/

auto
main() -> int {
    { // Test adding two column vectors
        ColumnInts veca {1, 2, 3};
        ColumnInts vecb {0, 8, 7};
        ColumnInts added {};

        exec<Add>(&added, &veca, &vecb);

        for (auto x : added) {
            std::cout << x << ' ';
        }
        std::cout << '\n';
    }

    { // Test a predicate
        ColumnInts veca {1, 0, 3};
        ColumnInts vecb {1, 8, 3};
        ColumnBools equals {};

        exec<Eq>(&equals, &veca, &vecb);

        for (auto x : equals) {
            std::cout << x << ' ';
        }
        std::cout << '\n';
    }

    { // Test a predicate with optionals
        MaybeNodeIDs a {0, std::nullopt, 1,            3, std::nullopt};
        MaybeNodeIDs b {0, std::nullopt, std::nullopt, 4, 2           };
        MaybeBools eqs {};

        exec<Eq>(&eqs, &a, &b);

        for (auto x : eqs) {
            if (!x.has_value()) std::cout << "_ ";
            else                std::cout << std::boolalpha << *x <<' ';
        } std::cout << '\n';
    }

    { // Example of using ColumnCombos for alloc'ing result col
        LocalMemory mem;
        ColumnInts a;
        ColumnInts b;

        using ResultColumn = ColumnCombination<Add, decltype(a), decltype(b)>::ResultColumnType;
        [[maybe_unused]] auto* res = mem.alloc<ResultColumn>();
    }
}
