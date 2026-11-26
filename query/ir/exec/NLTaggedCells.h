#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "ID.h"
#include "CellUtils.h"
#include "QueryTypeName.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "IRException.h"

namespace db {

inline std::optional<CellView> presentCell(const CellView element) {
    if (element.getTag() == CellTag::Null) {
        return std::nullopt;
    }

    return element;
}

// The number a tagged cell holds, whatever numeric type its tag names. A deviation over
// a type-erased column is defined over numbers only, so any other tag is a query error -
// the check the static column types make for a typed column, made per row here.
inline double taggedNumericValue(const CellView element) {
    const CellTag tag = element.getTag();

    switch (tag) {
        case CellTag::Int:
            return static_cast<double>(element.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return static_cast<double>(element.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return element.getAs<types::Double::Primitive>();
        break;

        default:
            throw IRException(fmt::format("stDev() and stDevP() read numbers, not '{}'", queryTypeName(tag)));
        break;
    }
}

// The list- or map-buffer value a tagged cell holds, read back as the type its tag names, so
// a collect of type-erased cells buffers each one under the type it came in with.
template <typename Item>
Item taggedItem(const CellView element) {
    const CellTag tag = element.getTag();

    switch (tag) {
        case CellTag::Int:
            return Item {element.getAs<types::Int64::Primitive>()};
        break;

        case CellTag::UInt:
            return Item {element.getAs<types::UInt64::Primitive>()};
        break;

        case CellTag::Double:
            return Item {element.getAs<types::Double::Primitive>()};
        break;

        case CellTag::Bool:
            return Item {element.getAs<types::Bool::Primitive>()};
        break;

        case CellTag::String:
            return Item {element.getAs<types::String::Primitive>()};
        break;

        case CellTag::Embedding:
            return Item {element.getAs<types::Embedding::Primitive>()};
        break;

        case CellTag::ListView:
            return Item {element.getAs<ListView>()};
        break;

        case CellTag::Null:
            return Item {element.getAs<PropertyNull>()};
        break;

        case CellTag::NodeID:
            return Item {element.getAs<NodeID>()};
        break;

        case CellTag::EdgeID:
            return Item {element.getAs<EdgeID>()};
        break;

        case CellTag::DateTime:
            return Item {element.getAs<types::DateTime::Primitive>()};
        break;

        case CellTag::Duration:
            return Item {element.getAs<types::Duration::Primitive>()};
        break;

        case CellTag::Point:
            return Item {element.getAs<types::Point::Primitive>()};
        break;

        case CellTag::MapView:
            return Item {element.getAs<MapView>()};
        break;

        case CellTag::Path:
            return Item {element.getAs<PathView>()};
        break;

        case CellTag::LabelID:
            return Item {element.getAs<LabelID>()};
        break;

        case CellTag::EdgeTypeID:
            return Item {element.getAs<EdgeTypeID>()};
        break;

        case CellTag::PropertyTypeID:
            return Item {element.getAs<PropertyTypeID>()};
        break;

        case CellTag::ValueType:
            return Item {element.getAs<ValueType>()};
        break;

        case CellTag::INVALID:
            throw IRException("cannot collect an untagged element");
        break;
    }

    throw IRException("Unknown CellTag");
}

}
