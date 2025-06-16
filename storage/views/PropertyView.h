#pragma once

#include "ID.h"
#include "metadata/PropertyType.h"

#include <variant>

namespace db {

using PropertyVariant = std::variant<const types::Int64::Primitive*,
                                     const types::UInt64::Primitive*,
                                     const types::Double::Primitive*,
                                     const types::String::Primitive*,
                                     const types::Bool::Primitive*>;

struct PropertyView {
    PropertyTypeID _id;
    PropertyVariant _value;

    template <SupportedType T>
    const T::Primitive& get() const {
        return *std::get<const typename T::Primitive*>(_value);
    }
};

}

