#pragma once

#include <stddef.h>
#include <optional>

#include "TuringProtoHeaders.h"

namespace wasm {

// The wire enums are used directly so the decoder can never drift from the encoder:
// ColumnWireHeader::_typeCode casts to ColumnType, _encoding casts to ColumnKind.
using ColumnKind = net::proto::ColumnKind;
using ColumnType = net::proto::ColumnInternalKind;

// Optionality is expressed through the element type, as in the native decoder:
// ColumnVector<std::optional<T>> is an OPTIONAL_VECTOR and ColumnConst<std::optional<T>>
// an OPTIONAL_CONSTANT, so all four wire kinds come from two class templates.
template <typename T>
inline constexpr bool isOptional = false;

template <typename T>
inline constexpr bool isOptional<std::optional<T>> = true;

class Column {
public:
    Column(ColumnKind kind, ColumnType type);
    virtual ~Column();

    ColumnKind getKind() const { return _kind; }
    ColumnType getInternalType() const { return _type; }

    virtual size_t size() const = 0;

    // Drops the column's decoded values, keeping the column itself: dataframes in a
    // response stream reuse the same columns.
    virtual void clear() = 0;

private:
    ColumnKind _kind {};
    ColumnType _type {};
};

}
