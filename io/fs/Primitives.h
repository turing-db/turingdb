#pragma once

#include <concepts>

namespace fs {

template <typename T>
concept CharPrimitive = std::same_as<T, char>
                     || std::same_as<T, signed char>
                     || std::same_as<T, unsigned char>
                     || std::same_as<T, wchar_t>
                     || std::same_as<T, char8_t>
                     || std::same_as<T, char16_t>
                     || std::same_as<T, char32_t>;

template <typename T>
concept TrivialPrimitive = std::is_trivial_v<T>
                        && std::is_standard_layout_v<T>;

template <typename T>
concept TrivialNonCharPrimitive = TrivialPrimitive<T> && !CharPrimitive<T>;

// Non-specialised trait declares all types as not being dumpable IDs
template <typename, typename = void>
struct is_dumpable_id : std::false_type {};

// Specialised trait declares all ID types (determine if T::Type) exists as dumpable IDs
template <typename T>
struct is_dumpable_id<T, std::void_t<typename T::Type>> : std::true_type {};

// Concept using the above traits
template <typename T>
concept DumpableID = is_dumpable_id<T>::value;

// Concept to constrain a trivial type to be dumped
template <typename T>
concept TriviallyDumpable = fs::TrivialPrimitive<T>;

// Dumpable concept: either an ID or a trivial type
template <typename T>
concept Dumpable = DumpableID<T> || TriviallyDumpable<T>;

// Traits to get the underlying type of an ID to dump/load it
template <typename T, bool IsID>
struct WorkingTypeHelper;

// If an ID type: the underlying type to dump is the ::Type property of the ID
template <typename T>
struct WorkingTypeHelper<T, true> { using type = typename T::Type; };

// If not an ID type: use the actual type as the type to dump
template <typename T>
struct WorkingTypeHelper<T, false> { using type = T; };

// Helper for Dump/Load: gets the underlying type of an ID or just the trivial type
template <typename T>
using WorkingType = typename WorkingTypeHelper<T, DumpableID<T>>::type;

}
