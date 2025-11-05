#pragma once

#include <type_traits>

template <typename KeyT, typename ValueT>
struct TypeValueMapPair {
    using key = KeyT;
    using value = ValueT;
};

template <typename... Types>
struct TypeValueMap {
    template <template <typename Key, typename Value> typename Transform, typename... Args>
    inline void transform(Args&&... args) {}
};

template <typename KeyT, typename ValueT, typename... Types>
struct TypeValueMap<TypeValueMapPair<KeyT, ValueT>, Types...> 
    : public TypeValueMap<Types...> {

    ValueT _value;

    template <typename OtherKeyT>
    inline constexpr auto& get() {
        if constexpr (std::is_same_v<OtherKeyT, KeyT>) {
            return _value;
        } else {
            return TypeValueMap<Types...>::template get<OtherKeyT>();
        }
    }

    template <template <typename Key, typename Value> typename Transform, typename... Args>
    inline void transform(Args&&... args) {
        using super = TypeValueMap<Types...>;

        Transform<KeyT, ValueT>{args...}(_value);
        super::template transform<Transform>(args...);
    }
};

