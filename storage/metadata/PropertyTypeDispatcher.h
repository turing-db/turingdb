#pragma once

#include "metadata/PropertyType.h"

#include "TuringException.h"

namespace db {

// Turns a runtime ValueType into the compile-time property type a templated
// operation needs: `PropertyTypeDispatcher {valueType}.execute(op)` calls
// `op.operator()<types::Int64>()`, `<types::String>()` and so on for the type the
// value carries. A value type no property can hold is malformed input and throws.
struct PropertyTypeDispatcher {
    ValueType _valueType {ValueType::Invalid};

    auto execute(const auto& executor) const {
        switch (_valueType) {
            case ValueType::Int64:
                return executor.template operator()<types::Int64>();
            case ValueType::UInt64:
                return executor.template operator()<types::UInt64>();
            case ValueType::Double:
                return executor.template operator()<types::Double>();
            case ValueType::Bool:
                return executor.template operator()<types::Bool>();
            case ValueType::String:
                return executor.template operator()<types::String>();
            case ValueType::Embedding:
                return executor.template operator()<types::Embedding>();
            case ValueType::_SIZE:
            case ValueType::Invalid: {
                throw TuringException("Unsupported property type");
            }
        }
    }
};

}
