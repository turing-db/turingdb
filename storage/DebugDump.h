#pragma once

#include <stdint.h>
#include <type_traits>
#include <ostream>

#include "ID.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyNull.h"

namespace db {

class DebugDump {
public:
    template <typename T>
    static constexpr bool fitsInWord = sizeof(T) <= sizeof(uint64_t);

    template <typename T>
    inline static void dump(std::ostream& out, T val) {
        if constexpr (fitsInWord<T>) {
            if constexpr (requires { T::_value; }) {
                dumpImpl(out, val._value);
            } else if constexpr (std::is_same_v<T, PropertyType>) {
                dumpImpl(out, val._id.getValue());
            } else {
                dumpImpl(out, (uint64_t)val);
            }
        }
    }

    template <typename T, int I>
    inline static void dump(std::ostream& out, ID<T, I> val) {
        if constexpr (fitsInWord<T>) {
            dumpImpl(out, val.getValue());
        }
    }

    template <typename T>
    inline static void dump(std::ostream& out, std::optional<T> val) {
        if (val) {
            dump(out, *val);
        } else {
            dumpNull(out);
        }
    }

    inline static void dump(std::ostream& out, PropertyNull) {
        dumpNull(out);
    }

    static void dump(std::ostream& out, std::string_view str);
    static void dump(std::ostream& out, const std::string& str);

    static void dumpString(std::ostream& out, const std::string& str);
    static void dumpNull(std::ostream& out);

private:
    static void dumpImpl(std::ostream& out, uint64_t data);
};

}
