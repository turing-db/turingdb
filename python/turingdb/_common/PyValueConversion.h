#pragma once

#include <stdint.h>
#include <span>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <nanobind/nanobind.h>

#include "NanobindUtils.h"
#include "PyOwnerBlocks.h"
#include "PyPandasTimes.h"
#include "CellView.h"
#include "CellUtils.h"
#include "GraphPath.h"
#include "PathView.h"
#include "OutputValues.h"
#include "list/ListView.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

namespace pybindings {

template <typename T>
int64_t temporalMicroseconds(T value) {
    return value.getMicroseconds();
}

// Converts a list or map value (or a single list element / map entry) to Python objects —
// scalars, a numpy float32 array for an embedding, a GraphPath for a path, a pd.Timestamp
// for a date, a pd.Timedelta for a duration, lists, and dicts for maps, each the type a row
// of a column of that value has. Nesting recurses through view() -> element()/entry() ->
// operator(); keeping them as members of one struct lets them call each other without a
// forward declaration.
struct ValueToPyObject {
    mutable PyPandasTimes _times;
    mutable nb::object _point;

    // One str per distinct map key, keyed by the bytes the key views, so the bytes must
    // outlive the converter. A dict does not hash a str again once it has its hash.
    mutable std::unordered_map<std::string_view, nb::str> _mapKeys;

    mutable PyOwnerBlocks<uint64_t> _pathIDs;
    mutable PyOwnerBlocks<float> _embeddings;

    void adoptPathIDs(std::vector<std::vector<uint64_t>>& blocks) { _pathIDs.adopt(blocks); }
    void adoptEmbeddings(std::vector<std::vector<float>>& blocks) { _embeddings.adopt(blocks); }

    nb::object duration(db::Duration value) const;
    nb::object dateTime(db::DateTime value) const;
    nb::object point(const db::Point& value) const;
    const nb::str& mapKey(std::string_view key) const;
    nb::object path(std::span<const uint64_t> ids) const;
    nb::object embedding(std::span<const float> values) const;

    nb::object view(const db::ListView& listView) const;
    nb::object element(const db::CellView element) const;
    nb::object view(const db::MapView& mapView) const;
    nb::object entry(const db::MapEntryView entry) const;

    template <typename T>
    nb::object operator()(const db::CellView element) const {
        if constexpr (std::is_same_v<T, db::types::Embedding::Primitive>) {
            return embedding(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::ListView>) {
            return view(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::MapView>) {
            return view(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::PropertyNull>) {
            return nb::none();
        } else if constexpr (std::is_same_v<T, db::types::Duration::Primitive>) {
            return duration(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::types::DateTime::Primitive>) {
            return dateTime(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::types::Point::Primitive>) {
            return point(element.getAs<T>());
        } else if constexpr (std::is_same_v<T, db::ValueType>) {
            return toPyString(db::ValueTypeName::value(element.getAs<T>()));
        } else if constexpr (std::is_same_v<T, db::PathView>) {
            return path(element.getAs<T>().getIDs());
        } else {
            return toPy(element.getAs<T>());
        }
    }

};

template <typename T>
nb::object toPyObject(const T& value, const ValueToPyObject& nested) {
    if constexpr (db::IsEmbedding<T>) {
        return nested.embedding(value);
    } else if constexpr (db::IsPath<T> || std::is_same_v<T, db::PathView>) {
        return nested.path(value.getIDs());
    } else if constexpr (db::IsListView<T> || db::IsMap<T>) {
        return nested.view(value);
    } else if constexpr (db::IsCell<T>) {
        return nested.element(value);
    } else if constexpr (db::IsNull<T>) {
        return nb::none();
    } else if constexpr (db::IsValueType<T>) {
        return toPyString(db::ValueTypeName::value(value));
    } else if constexpr (db::IsPoint<T>) {
        return nested.point(value);
    } else {
        return toPy(value);
    }
}

// A row's element in a numpy array, or its Python object where its type has no array form.
template <typename Row>
auto toPyElement(const Row& row, const ValueToPyObject& nested) {
    if constexpr (std::is_arithmetic_v<Row>) {
        return row;
    } else if constexpr (db::IsDateTime<Row> || db::IsDuration<Row>) {
        return temporalMicroseconds(row);
    } else if constexpr (std::is_same_v<Row, db::CustomBool>) {
        return static_cast<uint8_t>(row._boolean);
    } else if constexpr (db::IDLike<Row> && !db::EntityIDLike<Row>) {
        return row.getValue();
    } else if constexpr (db::Hash<Row>) {
        return row.get();
    } else {
        return toPyObject(row, nested);
    }
}

// pandas 2 reads bare integers given a timedelta64[us] dtype as nanoseconds,
// so an array of microseconds carries its unit itself.
template <typename Row>
nb::object withTemporalUnit(const nb::object& array) {
    if constexpr (db::IsDateTime<Row>) {
        return array.attr("view")("datetime64[us]");
    } else if constexpr (db::IsDuration<Row>) {
        return array.attr("view")("timedelta64[us]");
    } else {
        return array;
    }
}

}
