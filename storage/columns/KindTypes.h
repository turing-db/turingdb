#pragma once

#include <tuple>
#include <array>

namespace db {

// To use if a type is a template
template <template <typename> typename Container>
struct TemplateKind {
    template <typename T>
    using type = Container<T>;
};

template <typename...>
struct AllUniqueHelper;

template <>
struct AllUniqueHelper<> {
    static consteval bool value() { return true; }
};

template <typename T>
struct AllUniqueHelper<T> {
    static consteval bool value() { return true; }
};

template <typename T, typename... Rest>
struct AllUniqueHelper<T, Rest...> {
    static consteval bool value() {
        return (!(std::is_same_v<T, Rest> || ...)) && AllUniqueHelper<Rest...>::value();
    }
};

template <typename T, typename Tuple>
struct IndexOfHelper;

template <typename T, typename... Types>
struct IndexOfHelper<T, std::tuple<T, Types...>> {
    static constexpr std::size_t value = 0;
};

template <typename T, typename U, typename... Types>
struct IndexOfHelper<T, std::tuple<U, Types...>> {
    static constexpr std::size_t value = 1 + IndexOfHelper<T, std::tuple<Types...>>::value;
};

template <class>
struct InnerTypeHelper {
    using type = std::false_type;
};

template <template <class> class C, class U>
struct InnerTypeHelper<C<U>> {
    using type = U;
};

template <typename T>
struct InnerTypeHelper<T&> : InnerTypeHelper<T> {};

template <typename T>
struct InnerTypeHelper<T*> : InnerTypeHelper<T> {};

template <typename T>
struct InnerTypeHelper<const T> : InnerTypeHelper<T> {};

template <class>
struct OuterTypeHelper {
    using type = std::false_type;
};

template <template <class> class C, class U>
struct OuterTypeHelper<C<U>> {
    using type = TemplateKind<C>;
};

template <typename... Ts>
class KindTypes {
public:
    using Tuple = std::tuple<Ts...>;

    // Contains
    template <typename T>
    static consteval bool contains() {
        return (std::is_same_v<T, Ts> || ...);
    }

    template <template <typename> class T>
    static consteval bool contains() {
        return (std::is_same_v<T<Ts>, TemplateKind<T>> || ...);
    }

    // IndexOf
    template <typename T>
    static consteval std::size_t indexOf() {
        static_assert(contains<T>());
        return IndexOfHelper<T, Tuple>::value + 1;
    }

    // TypeAt
    template <std::size_t I>
    using TypeAt = std::tuple_element_t<I, Tuple>;

    // ALlUnique
    static consteval bool allUnique() {
        return AllUniqueHelper<Ts...>::value();
    }

    // Count
    static consteval std::size_t count() {
        return std::tuple_size_v<Tuple>;
    }

    static_assert(allUnique());
};

}
