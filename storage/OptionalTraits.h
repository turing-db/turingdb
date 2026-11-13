#pragma once

#include <optional>
#include <type_traits>

namespace db {

struct OptionalTraits {
    template <typename T>
    struct is_optional : std::false_type {};

    template <typename U>
    struct is_optional<std::optional<U>> : std::true_type {};

    template <typename T>
    static constexpr bool is_optional_v = is_optional<std::remove_cvref_t<T>>::value;

    template <typename T>
    struct unwrap_optional {
        using underlying_type = T;
    };

    template <typename U>
    struct unwrap_optional<std::optional<U>> {
        using underlying_type = U;
    };

    template <typename T>
    using unwrap_optional_t = typename unwrap_optional<std::remove_cvref_t<T>>::underlying_type;
};

}
