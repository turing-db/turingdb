#include <concepts>
#include <iostream>
#include <optional>

#include "ColumnCombinations.h"
#include "BinaryOperators.h"
#include "MaskOperators.h"

#include "LocalMemory.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnVector.h"
#include "columns/KindTypes.h"
#include "metadata/PropertyType.h"

using namespace db;

using ColumnInts = ColumnVector<types::Int64::Primitive>;
using ColumnBools = ColumnVector<bool>;
using MaybeNodeIDs = ColumnOptVector<NodeID>;
using MaybeBools = ColumnOptVector<bool>;

static_assert(std::same_as<InnerTypeHelper<ColumnInts>::type, types::Int64::Primitive>);

/*

Column operators:

switch (op) {
    case (OP_ADD): exec<Add>(res, lhs, rhs);
    case (OP_SUB): exec<Sub>(res, lhs, rhs);
}

*/

auto main() -> int {
    const auto expect_eq = [](auto const& got,
                              auto const& expected,
                              std::string_view test_name) {
        if (got.size() != expected.size()) {
            std::cerr << "[FAIL] " << test_name
                      << " size mismatch: got " << got.size()
                      << ", expected " << expected.size() << '\n';
            std::exit(EXIT_FAILURE);
        }

        auto it_g = got.begin();
        auto it_e = expected.begin();
        std::size_t i = 0;

        for (; it_g != got.end(); ++it_g, ++it_e, ++i) {
            if (*it_g != *it_e) {
                std::cerr << "[FAIL] " << test_name << '\n';
                std::exit(EXIT_FAILURE);
            }
        }

        std::cerr << "[PASS] " << test_name << '\n';
    };

    { // Add column vectors
        ColumnInts veca {1, 2, 3};
        ColumnInts vecb {0, 8, 7};
        ColumnInts added {};

        exec<Add>(&added, &veca, &vecb);

        ColumnInts expected {1, 10, 10};
        expect_eq(added, expected, "Add ColumnInts");
    }

    { // Predicate Eq
        ColumnInts veca {1, 0, 3};
        ColumnInts vecb {1, 8, 3};
        ColumnBools equals {};

        exec<Eq>(&equals, &veca, &vecb);

        ColumnBools expected {true, false, true};
        expect_eq(equals, expected, "Eq ColumnInts");
    }

    { // Predicate Eq with optionals
        MaybeNodeIDs a {0, std::nullopt, 1,            3, std::nullopt};
        MaybeNodeIDs b {0, std::nullopt, std::nullopt, 4, 2           };
        MaybeBools eqs {};

        exec<Eq>(&eqs, &a, &b);

        MaybeBools expected {
            true,
            std::nullopt,
            std::nullopt,
            false,
            std::nullopt
        };

        expect_eq(eqs, expected, "Eq MaybeNodeIDs");
    }

    { // ColumnCombination allocation
        LocalMemory mem;
        ColumnInts a;
        ColumnInts b;

        using ResultColumn =
            ColumnCombination<Add, decltype(a), decltype(b)>::ResultColumnType;
        [[maybe_unused]] auto* res = mem.alloc<ResultColumn>();
    }

    { // AND on masks
        ColumnMask a {true, false, true, false};
        ColumnMask b {true, false, false, true};
        ColumnMask res {};

        exec<AND>(&res, &a, &b);

        ColumnMask expected {true, false, false, false};
        expect_eq(res, expected, "AND ColumnMask");
    }

    { // OR on masks
        ColumnMask a {true, false, true, false};
        ColumnMask b {true, false, false, true};
        ColumnMask res {};

        exec<OR>(&res, &a, &b);

        ColumnMask expected {true, false, true, true};
        expect_eq(res, expected, "OR ColumnMask");
    }

    { // NOT on masks
        ColumnMask a {true, false, true, false};
        ColumnMask res {};

        exec<NOT>(&res, &a);

        ColumnMask expected {false, true, false, true};
        expect_eq(res, expected, "NOT ColumnMask");
    }

    { // Apply mask
        ColumnMask mask {true, false, true};
        ColumnInts src {-10, 10, -20};
        ColumnInts result {};

        exec<Apply>(&result, &mask, &src);

        ColumnInts expected {-10, -20};
        expect_eq(result, expected, "Apply mask");
    }
}
