#pragma once

#include <type_traits>

#include "GraphPath.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"

namespace net::proto {

template <typename T>
concept TrivialInternalTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::DateTime::Primitive>
|| std::is_same_v<T, db::types::Duration::Primitive>
|| std::is_same_v<T, db::types::Point::Primitive>
|| std::is_same_v<T, db::ValueType>
|| std::is_same_v<T, db::NodeID>
|| std::is_same_v<T, db::EdgeID>
|| std::is_same_v<T, db::EdgeTypeID>
|| std::is_same_v<T, db::PropertyTypeID>
|| std::is_same_v<T, db::LabelID>
|| std::is_same_v<T, db::LabelSetID>
|| std::is_same_v<T, db::ChangeID>;

template <typename T, typename Sink>
concept VariableWidthTypes = std::is_same_v<T, db::types::String::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, typename Sink::ListView>
|| std::is_same_v<T, typename Sink::CellView>
|| std::is_same_v<T, typename Sink::MapView>;

// A column decodes when Decoders.h has a decoder for its element in its encoding and the sink
// can hold the column
template <typename T, typename Sink>
concept SupportedColumnVectorTypes = (TrivialInternalTypes<T> || VariableWidthTypes<T, Sink> || std::is_same_v<T, db::GraphPath>)
&& Sink::template holds<typename Sink::template ColumnVector<T>>();

// A tagged cell holds its own null, so no column of cells is sent as an optional one
template <typename T, typename Sink>
concept SupportedColumnOptVectorTypes = (TrivialInternalTypes<T> || VariableWidthTypes<T, Sink>)
&& !std::is_same_v<T, typename Sink::CellView>
&& Sink::template holds<typename Sink::template ColumnOptVector<T>>();

template <typename T, typename Sink>
concept SupportedColumnConstTypes = (TrivialInternalTypes<T> || VariableWidthTypes<T, Sink> || std::is_same_v<T, db::GraphPath> || std::is_same_v<T, db::PropertyNull>)
&& Sink::template holds<typename Sink::template ColumnConst<T>>();

template <typename T, typename Sink>
concept SupportedColumnOptConstTypes = (TrivialInternalTypes<T> || VariableWidthTypes<T, Sink> || std::is_same_v<T, db::GraphPath>)
&& !std::is_same_v<T, typename Sink::CellView>
&& Sink::template holds<typename Sink::template ColumnOptConst<T>>();

}
