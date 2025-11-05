#pragma once

#include <algorithm>
#include <array>
#include <string_view>

/**
 * @brief Constexpr version of a string literal
 *
 * Can be used as a template argument (as opposed to const char*)
 */
template <std::size_t N>
struct EnumStringLiteral {
    constexpr EnumStringLiteral(const char (&str)[N]) {
        std::copy(str, str + N, value);
    }

    char value[N] {};
};

template <auto VEnumValue, EnumStringLiteral VStringValue>
struct EnumStringPair {
    static constexpr auto EnumValue = VEnumValue;
    static constexpr auto StringValue = VStringValue;
};

template <typename T>
concept EnumStringPairTrait = requires { T::EnumValue; T::StringValue; };

template <typename TEnum>
struct EnumToString {
    template <EnumStringPairTrait... TPairs>
    struct Create {
        static constexpr std::array<TEnum, sizeof...(TPairs)> EnumValues {TPairs::EnumValue...};
        using EnumArrayType = decltype(EnumValues);
        static constexpr std::array<std::string_view, sizeof...(TPairs)> Strings {TPairs::StringValue.value...};
        using EnumSequence = std::integer_sequence<TEnum, TPairs::EnumValue...>;

        static constexpr std::size_t EnumCount = EnumValues.size();

        static consteval bool containsOnlyUnique(EnumArrayType arr) {
            auto seq = arr;
            std::sort(seq.begin(), seq.end());
            auto* it = std::unique(seq.begin(), seq.end());
            return (it == seq.end());
        }

        static constexpr std::string_view value(TEnum e) {
            return Strings[(std::size_t)e];
        }

        static_assert(containsOnlyUnique(EnumValues),
                      "Definition cannot have duplicate enum entries");
        static_assert(EnumCount == (std::size_t)TEnum::_SIZE,
                      "All enums must be provided a name");
    };
};
