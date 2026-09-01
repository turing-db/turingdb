#pragma once

#include <string>
#include <type_traits>

#include "EntityList.h"
#include "GraphPath.h"
#include "list/ListView.h"
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

template <typename T>
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
|| std::is_same_v<T, db::ListElementView>
|| std::is_same_v<T, db::ListView>
|| std::is_same_v<T, std::string>;

template <typename T>
concept SupportedColumnOptVectorTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, std::string>;

template <typename T>
concept SupportedColumnConstTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, db::PropertyNull>
|| std::is_same_v<T, db::ListView>
|| std::is_same_v<T, std::string>;
//|| std::is_same_v<T, db::ListElementView> - disabled: no ColumnConst<ListElementView> memory pool

template <typename T>
concept SupportedColumnOptConstTypes = std::is_same_v<T, db::types::UInt64::Primitive>
|| std::is_same_v<T, db::types::Int64::Primitive>
|| std::is_same_v<T, db::types::Double::Primitive>
|| std::is_same_v<T, db::types::Bool::Primitive>
|| std::is_same_v<T, db::types::Embedding::Primitive>
|| std::is_same_v<T, std::string>;

}
