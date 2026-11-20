// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <optional>

#include "TuringProtoDecoderConcepts.h"
#include "CellTag.h"
#include "CellUtils.h"

#include "Column.h"
#include "ColumnConst.h"
#include "ColumnContainer.h"
#include "ColumnOptVector.h"
#include "ColumnVector.h"
#include "WasmCellView.h"
#include "WasmValueStore.h"
#include "ListView.h"

namespace wasm {

template <typename T>
struct ElementOf {
    using Type = T;
};

template <typename T>
struct ElementOf<std::optional<T>> {
    using Type = T;
};

// Maps a column's element type back to its wire type code, so sink-allocated columns
// are self-describing.
template <typename T>
consteval ColumnType wireCodeOfElement() {
    using Element = typename ElementOf<T>::Type;

    if constexpr (std::is_same_v<Element, db::types::UInt64::Primitive>) {
        return ColumnType::UINT64;
    } else if constexpr (std::is_same_v<Element, db::types::Int64::Primitive>) {
        return ColumnType::INT64;
    } else if constexpr (std::is_same_v<Element, db::types::Double::Primitive>) {
        return ColumnType::DOUBLE;
    } else if constexpr (std::is_same_v<Element, db::types::Bool::Primitive>) {
        return ColumnType::BOOL;
    } else if constexpr (std::is_same_v<Element, db::types::String::Primitive>) {
        return ColumnType::STRING;
    } else if constexpr (std::is_same_v<Element, db::types::Embedding::Primitive>) {
        return ColumnType::EMBEDDING;
    } else if constexpr (std::is_same_v<Element, db::types::DateTime::Primitive>) {
        return ColumnType::DATE_TIME;
    } else if constexpr (std::is_same_v<Element, db::types::Duration::Primitive>) {
        return ColumnType::DURATION;
    } else if constexpr (std::is_same_v<Element, db::types::Point::Primitive>) {
        return ColumnType::POINT;
    } else if constexpr (std::is_same_v<Element, db::GraphPath>) {
        return ColumnType::GRAPH_PATH;
    } else if constexpr (std::is_same_v<Element, ListView>) {
        return ColumnType::LIST_VIEW;
    } else if constexpr (std::is_same_v<Element, WasmCellView>) {
        return ColumnType::CELL_VIEW;
    } else if constexpr (std::is_same_v<Element, MapView>) {
        return ColumnType::MAP_VIEW;
    } else if constexpr (std::is_same_v<Element, db::ValueType>) {
        return ColumnType::VALUE_TYPE;
    } else if constexpr (std::is_same_v<Element, db::PropertyNull>) {
        return ColumnType::PROPERTY_NULL;
    } else if constexpr (std::is_same_v<Element, db::NodeID>) {
        return ColumnType::NODE_ID;
    } else if constexpr (std::is_same_v<Element, db::EdgeID>) {
        return ColumnType::EDGE_ID;
    } else if constexpr (std::is_same_v<Element, db::EdgeTypeID>) {
        return ColumnType::EDGE_TYPE_ID;
    } else if constexpr (std::is_same_v<Element, db::PropertyTypeID>) {
        return ColumnType::PROPERTY_TYPE_ID;
    } else if constexpr (std::is_same_v<Element, db::LabelID>) {
        return ColumnType::LABEL_ID;
    } else if constexpr (std::is_same_v<Element, db::LabelSetID>) {
        return ColumnType::LABEL_SET_ID;
    } else if constexpr (std::is_same_v<Element, db::ChangeID>) {
        return ColumnType::CHANGE_ID;
    } else {
        static_assert(sizeof(Element) == 0, "No wire type code for this element type");
    }
}

class WasmSink {
public:
    using ColumnContainer = wasm::ColumnContainer;
    using ValueStore = WasmValueStore;
    using Column = wasm::Column;

    template <typename T>
    using ColumnVector = wasm::ColumnVector<T>;

    template <typename T>
    using ColumnOptVector = wasm::ColumnOptVector<T>;

    template <typename T>
    using ColumnConst = wasm::ColumnConst<T>;

    template <typename T>
    using ColumnOptConst = wasm::ColumnConst<std::optional<T>>;

    using ListView = wasm::ListView;
    using CellView = wasm::WasmCellView;
    using MapView = wasm::MapView;

    WasmSink();
    ~WasmSink();

    template <typename ColumnT>
    Column* alloc() { return new ColumnT(wireCodeOfElement<typename ColumnT::ValueType>()); }

    template <typename ColumnT>
    static consteval bool holds() { return true; }

    WasmValueStore* getValues() { return &_values; }

private:
    WasmValueStore _values;
};

template <typename T>
concept WasmOptionalVectorColumn = net::proto::OptionalVectorColumn<WasmSink::ColumnOptVector<T>, T>;

static_assert(WasmOptionalVectorColumn<db::types::UInt64::Primitive>);
static_assert(WasmOptionalVectorColumn<db::types::Int64::Primitive>);
static_assert(WasmOptionalVectorColumn<db::types::Double::Primitive>);
static_assert(WasmOptionalVectorColumn<db::types::Bool::Primitive>);
static_assert(WasmOptionalVectorColumn<db::types::DateTime::Primitive>);
static_assert(WasmOptionalVectorColumn<db::types::Duration::Primitive>);
static_assert(WasmOptionalVectorColumn<db::types::Point::Primitive>);
static_assert(WasmOptionalVectorColumn<db::NodeID>);
static_assert(WasmOptionalVectorColumn<db::types::String::Primitive>);
static_assert(WasmOptionalVectorColumn<db::types::Embedding::Primitive>);
static_assert(WasmOptionalVectorColumn<WasmSink::ListView>);
static_assert(WasmOptionalVectorColumn<WasmSink::MapView>);

}
