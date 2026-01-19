#include <concepts>
#include <iostream>

#include "PropertyOperators.h"
#include "columns/ColumnOptMask.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

using namespace db;

using ColumnInts = ColumnVector<types::Int64::Primitive>;

static_assert(std::same_as<contained_type<ColumnInts*>::type, types::Int64::Primitive>);

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

    // {
    //     ColumnInts veca {1, 0, 3};
    //     ColumnInts vecb {1, 8, 3};
    //     ColumnInts equals {};

    //     exec<Eq>(&equals, &veca, &vecb);

    //     for (auto x : equals) {
    //         std::cout << x << ' ';
    //     }
    //     std::cout << '\n';

    //     /* for (auto x : equals) {
    //         if (!x.has_value()) {
    //             std::cout << "_ ";
    //         } else
    //             std::cout << *x << ' ';
    //     }
    //     std::cout << '\n'; */
    // }

    
}
