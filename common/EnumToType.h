#pragma once

#include "UniqueTuple.h"

#include <algorithm>

template <auto TEnumValue, typename TType>
struct EnumTypePair {
    using Type = TType;
    static constexpr auto EnumValue = TEnumValue;
};

template <typename T>
concept EnumTypePairTrait = requires { T::EnumValue; typename T::Type; };

template <typename TEnum>
struct EnumToType {
    template <EnumTypePairTrait... TPairs>
    struct Details {
        static constexpr std::array<TEnum, sizeof...(TPairs)> EnumValues {TPairs::EnumValue...};
        using EnumArrayType = decltype(EnumValues);
        using InternalTypes = std::tuple<typename TPairs::Type...>;
        using InternalUniqueTypes = MakeUniqueTuple<InternalTypes>::Type;
        using EnumSequence = std::integer_sequence<TEnum, TPairs::EnumValue...>;

        static constexpr std::size_t Count = std::tuple_size_v<InternalTypes>;
        static constexpr std::size_t UniqueTypeCount = std::tuple_size_v<InternalUniqueTypes>;
        static constexpr std::size_t EnumCount = EnumValues.size();

        static consteval bool containsOnlyUnique(EnumArrayType arr) {
            auto seq = arr;
            std::sort(seq.begin(), seq.end());
            auto* it = std::unique(seq.begin(), seq.end());
            return (it == seq.end());
        }

        static_assert(containsOnlyUnique(EnumValues)
                      && "Definition cannot have duplicate enum entries");
        static_assert(EnumCount == (std::size_t)TEnum::_SIZE && ""
                      && "All enums must be provided a type");

        template <TEnum e>
        using Type = std::tuple_element<(std::size_t)e, InternalTypes>::type;
    };

    template <EnumTypePairTrait... TPairs>
    using Create = Details<TPairs...>;
};
