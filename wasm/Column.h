// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stddef.h>
#include <optional>

#include "TuringProtoHeaders.h"

namespace wasm {

// The wire enums are used directly so the decoder can never drift from the encoder:
// ColumnWireHeader::_typeCode casts to ColumnType, _encoding casts to ColumnKind.
using ColumnKind = net::proto::ColumnKind;
using ColumnType = net::proto::ColumnInternalKind;

// ColumnConst<std::optional<T>> is an OPTIONAL_CONSTANT. An OPTIONAL_VECTOR is a
// ColumnOptVector, which keeps values and validity apart.
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
