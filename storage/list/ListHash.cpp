#include "ListHash.h"

#include <math.h>
#include <functional>
#include <limits>

#include "ListBufferTypeTag.h"

#include "ID.h"
#include "map/MapHash.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

size_t combine(size_t seed, size_t value) {
    return seed * 31 + value;
}

template <typename T>
size_t hashValue(const T& value) {
    return std::hash<T> {}(value);
}

}

size_t db::hashNumber(double value) {
    if (value == 0.0) {
        value = 0.0;
    } else if (std::isnan(value)) {
        value = std::numeric_limits<double>::quiet_NaN();
    }

    return combine(static_cast<size_t>(ListBufferTypeTag::Double), hashValue(value));
}

size_t db::hashListElement(ListElementView element) {
    const ListBufferTypeTag tag = element.getTag();
    const size_t seed = static_cast<size_t>(tag);

    switch (tag) {
        case ListBufferTypeTag::Int:
            return hashNumber(static_cast<double>(element.getAs<types::Int64::Primitive>()));
        break;
        case ListBufferTypeTag::UInt:
            return hashNumber(static_cast<double>(element.getAs<types::UInt64::Primitive>()));
        break;
        case ListBufferTypeTag::Double:
            return hashNumber(element.getAs<types::Double::Primitive>());
        break;
        case ListBufferTypeTag::Bool:
            return combine(seed, hashValue(element.getAs<types::Bool::Primitive>()));
        break;
        case ListBufferTypeTag::String:
            return combine(seed, hashValue(element.getAs<types::String::Primitive>()));
        break;
        case ListBufferTypeTag::NodeID:
            return combine(seed, hashValue(element.getAs<NodeID>().getValue()));
        break;
        case ListBufferTypeTag::EdgeID:
            return combine(seed, hashValue(element.getAs<EdgeID>().getValue()));
        break;
        case ListBufferTypeTag::DateTime:
            return combine(seed, hashValue(element.getAs<types::DateTime::Primitive>()));
        break;
        case ListBufferTypeTag::Embedding: {
            size_t hash = seed;
            for (const float value : element.getAs<types::Embedding::Primitive>()) {
                hash = combine(hash, hashValue(value));
            }
            return hash;
        }
        break;
        case ListBufferTypeTag::ListView:
            return combine(seed, hashList(element.getAs<ListView>()));
        break;
        case ListBufferTypeTag::MapView:
            return combine(seed, hashMap(element.getAs<MapView>()));
        break;
        case ListBufferTypeTag::Null:
        case ListBufferTypeTag::INVALID:
            return seed;
        break;
    }

    return seed;
}

size_t db::hashList(ListView list) {
    size_t hash = list.size();

    for (const ListElementView element : list) {
        hash = combine(hash, hashListElement(element));
    }

    return hash;
}
