#pragma once

#include <type_traits>

#include "EntityList.h"
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
|| std::is_same_v<T, db::ValueType>
|| std::is_same_v<T, db::NodeID>
|| std::is_same_v<T, db::EdgeID>
|| std::is_same_v<T, db::EdgeTypeID>
|| std::is_same_v<T, db::PropertyTypeID>
|| std::is_same_v<T, db::LabelID>
|| std::is_same_v<T, db::LabelSetID>
|| std::is_same_v<T, db::ChangeID>;

template <typename T, typename Sink>
concept SupportedColumnVectorTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, db::ValueType>
|| std::is_same_v<T, db::NodeID>
|| std::is_same_v<T, db::EdgeID>
|| std::is_same_v<T, db::EdgeTypeID>
|| std::is_same_v<T, db::PropertyTypeID>
|| std::is_same_v<T, db::LabelID>
|| std::is_same_v<T, db::LabelSetID>
|| std::is_same_v<T, db::ChangeID>
|| std::is_same_v<T, db::Path>
|| std::is_same_v<T, db::EntityList>
|| std::is_same_v<T, typename Sink::ListElementView>
|| std::is_same_v<T, typename Sink::ListView>
|| std::is_same_v<T, db::types::String::Primitive>;

template <typename T, typename Sink>
concept SupportedColumnOptVectorTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, db::types::String::Primitive>;

template <typename T, typename Sink>
concept SupportedColumnConstTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, db::PropertyNull>
|| std::is_same_v<T, typename Sink::ListView>
|| std::is_same_v<T, db::types::String::Primitive>;
//|| std::is_same_v<T, typename Sink::ListElementView> - disabled: no ColumnConst<ListElementView> memory pool

template <typename T, typename Sink>
concept SupportedColumnOptConstTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, db::types::String::Primitive>;

}
