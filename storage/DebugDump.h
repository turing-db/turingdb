#pragma once

#include <stdint.h>
#include <type_traits>

#include "ID.h"
#include "metadata/PropertyType.h"

namespace db {

class DebugDump {
public:
    template <typename T>
    static constexpr bool fitsInWord = sizeof(T) <= sizeof(uint64_t);

    template <typename T>
    inline static void dump(T val) {
        if constexpr (fitsInWord<T>) {
            if constexpr (std::is_same_v<T, EntityID>
                          || std::is_same_v<T, NodeID>
                          || std::is_same_v<T, EdgeID>
                          || std::is_same_v<T, EdgeTypeID>
                          || std::is_same_v<T, PropertyTypeID>
                          || std::is_same_v<T, LabelSetID>) {
                dumpImpl(val.getValue());
            } else if constexpr (requires { T::_value; }) {
                dumpImpl(val._value);
            } else if constexpr (std::is_same_v<T, PropertyType>) {
                dumpImpl(val._id.getValue());
            } else {
                dumpImpl((uint64_t)val);
            }
        }
    }

    template <typename T>
    inline static void dump(std::optional<T> val) {
        if (val) {
            dump(*val);
        } else {
            dumpNull();
        }
    }

    static void dumpString(const std::string& str);
    static void dumpNull();

private:
    static void dumpImpl(uint64_t data);
};

}
