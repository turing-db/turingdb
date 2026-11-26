#include "NLExecutor.h"

#include <stdint.h>
#include <bit>
#include <cmath>
#include <concepts>
#include <limits>
#include <span>
#include <string_view>

#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "CellOrder.h"
#include "metadata/PropertyType.h"

#include "NLSerializedKey.h"
#include "NLProgram.h"
#include "NLRuntimeProperty.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// Append the raw bytes of a present property value to a distinct row key. The value
// is appended verbatim: a trivially-copyable primitive copies its object bytes; a
// string copies a length prefix then its characters, so two rows never collide by
// concatenation (so "a"+"b" and "ab"+"" get distinct keys). A view - a list, a map - would
// key on its own bytes, a pointer into its buffer, so it keys through its own serializer.
template <typename Primitive>
    requires (!NLViewElement<Primitive>)
void distinctAppendValueBytes(NLSerializedKey& key, const Primitive& value) {
    key.append(reinterpret_cast<const char*>(&value), sizeof(Primitive));
}

// A float keys by its Cypher value, not its bit pattern: -0.0 is +0.0 (0.0 == -0.0 in
// Cypher) and every NaN payload maps to one canonical NaN, so two distinct bit patterns of
// the same value never split into separate groups (or survive DISTINCT as two rows).
template <std::floating_point Float>
Float normalizedKeyFloat(const Float value) {
    if (value == 0) {
        return 0;
    } else if (std::isnan(value)) {
        return std::numeric_limits<Float>::quiet_NaN();
    }

    return value;
}

template <std::floating_point Float>
void distinctAppendFloatBytes(NLSerializedKey& key, const Float value) {
    const Float normalized = normalizedKeyFloat(value);
    key.append(reinterpret_cast<const char*>(&normalized), sizeof(normalized));
}

void distinctAppendValueBytes(NLSerializedKey& key, double value) {
    distinctAppendFloatBytes(key, value);
}

void distinctAppendValueBytes(NLSerializedKey& key, std::string_view value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));
    key.append(value.data(), value.size());
}

void distinctAppendValueBytes(NLSerializedKey& key, const types::Embedding::Primitive value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));

    for (const float element : value) {
        distinctAppendFloatBytes(key, element);
    }
}

// Serialize one row of an ID column (node/edge/edge-type IDs) into the row key as
// the ID's underlying integer value, byte for byte.
template <typename ElementType>
void distinctKeyAppendColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const auto value = raw[row].getValue();
    distinctAppendValueBytes(key, value);
}

// Serialize one row of a chunk that holds its values plainly - a column a procedure
// yielded, a tally, an expression over one - into the row key. The ID sibling reads
// through the ID's integer; here the element is the value, and no row of such a chunk is
// null, so the key carries no tag byte to tell a null from a value.
template <typename ElementType>
void distinctKeyAppendPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    distinctAppendValueBytes(key, raw[row]);
}

void distinctAppendListBytes(NLSerializedKey& key, const ListView list);
void distinctAppendMapBytes(NLSerializedKey& key, const MapView map);

template <typename Primitive>
void distinctAppendPresentValue(NLSerializedKey& key, const Primitive& value) {
    key.push_back('\1');
    distinctAppendValueBytes(key, value);
}

void distinctAppendPresentValue(NLSerializedKey& key, const ListView list) {
    distinctAppendListBytes(key, list);
}

void distinctAppendPresentValue(NLSerializedKey& key, const MapView map) {
    distinctAppendMapBytes(key, map);
}

// Serialize one row of a nullable value column into the row key as a tag byte telling
// null from present, then, when present, the value's bytes. A null serializes to the
// tag alone, so all nulls share a key and DISTINCT dedups them together, matching Cypher.
template <typename Primitive>
void distinctKeyAppendOptColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back('\0');
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValues()[row]);
}

// What a key value packs as, holding the value as its serialized key does: an ID's integer, a
// number's own bits, a double's once normalized, a boolean's byte, the microseconds of a
// datetime or a duration, and a point's own bytes
template <IntegralType T, int I>
T packedKeyValue(ID<T, I> id) {
    return id.getValue();
}

types::Int64::Primitive packedKeyValue(types::Int64::Primitive value) {
    return value;
}

types::UInt64::Primitive packedKeyValue(types::UInt64::Primitive value) {
    return value;
}

uint64_t packedKeyValue(types::Double::Primitive value) {
    return std::bit_cast<uint64_t>(normalizedKeyFloat(value));
}

uint8_t packedKeyValue(types::Bool::Primitive value) {
    return value._boolean;
}

uint8_t packedKeyValue(ValueType value) {
    return static_cast<uint8_t>(value);
}

int64_t packedKeyValue(types::DateTime::Primitive value) {
    return value.getMicroseconds();
}

int64_t packedKeyValue(types::Duration::Primitive value) {
    return value.getMicroseconds();
}

types::Point::Primitive packedKeyValue(types::Point::Primitive value) {
    return value;
}

template <typename ElementType>
void packKeyColumn(const Column* column, size_t beginRow, size_t endRow, const NLKeyPackTarget& target) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    std::byte* value = target._values;

    for (size_t row = beginRow; row < endRow; row++) {
        const auto packed = packedKeyValue(raw[row]);
        std::memcpy(value, &packed, sizeof(packed));

        value += target._valueStride;
    }
}

template <typename Primitive>
void packOptKeyColumn(const Column* column, size_t beginRow, size_t endRow, const NLKeyPackTarget& target) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);
    const std::vector<Primitive>& values = typedColumn->getValues();
    const std::byte nullMask {static_cast<uint8_t>(1u << (target._nullBit % 8))};

    const auto markNulls = [&](size_t first, size_t last) {
        std::byte* nulls = target._nulls + target._nullBit / 8 + (first - beginRow) * target._nullStride;

        for (size_t row = first; row < last; row++) {
            *nulls |= nullMask;
            nulls += target._nullStride;
        }
    };

    size_t nullFirst = beginRow;
    typedColumn->forEachPresentRun(beginRow, endRow, [&](size_t first, size_t last) {
        markNulls(nullFirst, first);

        std::byte* value = target._values + (first - beginRow) * target._valueStride;
        for (size_t row = first; row < last; row++) {
            const auto packed = packedKeyValue(values[row]);
            std::memcpy(value, &packed, sizeof(packed));

            value += target._valueStride;
        }

        nullFirst = last;
    });

    markNulls(nullFirst, endRow);
}

template <typename ElementType>
NLKeyPacking keyPacking() {
    using Packed = decltype(packedKeyValue(std::declval<ElementType>()));

    return {&packKeyColumn<ElementType>, sizeof(Packed)};
}

template <typename Primitive>
NLKeyPacking optKeyPacking() {
    using Packed = decltype(packedKeyValue(std::declval<Primitive>()));

    return {&packOptKeyColumn<Primitive>, sizeof(Packed)};
}

void distinctAppendElementBytes(NLSerializedKey& key, CellView element);

// The count fixes how many IDs follow it, so they need no separator.
void distinctAppendPathBytes(NLSerializedKey& key, std::span<const uint64_t> ids) {
    const size_t idCount = ids.size();
    key.append(reinterpret_cast<const char*>(&idCount), sizeof(idCount));
    key.append(reinterpret_cast<const char*>(ids.data()), ids.size_bytes());
}

// Serialize a nested list into the row key as its length then its elements, so a list
// never keys the same as the concatenation of its neighbours.
void distinctAppendListBytes(NLSerializedKey& key, const ListView list) {
    const size_t size = list.size();
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const CellView element : list) {
        distinctAppendElementBytes(key, element);
    }
}

// Serialize a number by its value rather than by the type it is tagged with: an integer
// and a float holding the same value are one Cypher value, so a float with no fractional
// part keys as that integer and only a fractional one keys as a double.
// A number keys under the Int tag whenever it holds one, so 1, 1.0 and a small unsigned 1
// all key alike - which is what makes the key agree with numeric equality.
void distinctAppendIntegerKey(NLSerializedKey& key, const types::Int64::Primitive value) {
    key.push_back(static_cast<char>(CellTag::Int));
    distinctAppendValueBytes(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::Int64::Primitive value) {
    distinctAppendIntegerKey(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::UInt64::Primitive value) {
    if (value <= static_cast<types::UInt64::Primitive>(std::numeric_limits<types::Int64::Primitive>::max())) {
        distinctAppendIntegerKey(key, static_cast<types::Int64::Primitive>(value));
        return;
    }

    key.push_back(static_cast<char>(CellTag::UInt));
    distinctAppendValueBytes(key, value);
}

void distinctAppendNumberKey(NLSerializedKey& key, const types::Double::Primitive value) {
    constexpr types::Double::Primitive integerBound = 9223372036854775808.0;
    constexpr types::Double::Primitive unsignedBound = 18446744073709551616.0;

    const bool holdsAWholeNumber = std::isfinite(value) && std::trunc(value) == value;
    const bool holdsAnInteger = holdsAWholeNumber && value >= -integerBound && value < integerBound;
    const bool holdsAnUnsigned = holdsAWholeNumber && value >= integerBound && value < unsignedBound;

    if (holdsAnInteger) {
        distinctAppendIntegerKey(key, static_cast<types::Int64::Primitive>(value));
    } else if (holdsAnUnsigned) {
        distinctAppendNumberKey(key, static_cast<types::UInt64::Primitive>(value));
    } else {
        key.push_back(static_cast<char>(CellTag::Double));
        distinctAppendValueBytes(key, value);
    }
}

void distinctAppendNumberBytes(NLSerializedKey& key, const CellView element) {
    switch (element.getTag()) {
        case CellTag::Int:
            return distinctAppendNumberKey(key, element.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return distinctAppendNumberKey(key, element.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return distinctAppendNumberKey(key, element.getAs<types::Double::Primitive>());
        break;

        default:
            bioassert(false, "Keying a non-numeric element as a number");
        break;
    }
}

// Serialize a map into the row key as its size, then each entry's key length, key bytes and
// value. Both producers store entries in key order, so equal maps serialize alike.
void distinctAppendMapBytes(NLSerializedKey& key, const MapView map) {
    const size_t size = map.size();
    key.append(reinterpret_cast<const char*>(&size), sizeof(size));

    for (const MapEntryView entry : map) {
        const std::string_view entryKey = entry.getKey();
        const size_t keySize = entryKey.size();
        key.append(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
        key.append(entryKey);

        distinctAppendElementBytes(key, entry.getValue());
    }
}

// Serialize one tagged scalar into the row key: its tag, then its value's bytes. Cells of
// two types never collide because the tag leads, and a null is the tag alone, so all
// nulls dedup together as they do in a nullable value column.
void distinctAppendElementBytes(NLSerializedKey& key, const CellView element) {
    const CellTag tag = element.getTag();

    switch (tag) {
        case CellTag::Int:
        case CellTag::UInt:
        case CellTag::Double:
            return distinctAppendNumberBytes(key, element);
        break;

        case CellTag::Bool:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, static_cast<bool>(element.getAs<types::Bool::Primitive>()));
            return;
        break;

        case CellTag::String:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::String::Primitive>());
            return;
        break;

        case CellTag::ListView:
            key.push_back(static_cast<char>(tag));
            distinctAppendListBytes(key, element.getAs<ListView>());
            return;
        break;

        case CellTag::Null:
            key.push_back(static_cast<char>(tag));
            return;
        break;

        case CellTag::NodeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<NodeID>().getValue());
            return;
        break;

        case CellTag::EdgeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<EdgeID>().getValue());
            return;
        break;

        case CellTag::DateTime:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::DateTime::Primitive>().getMicroseconds());
            return;
        break;

        case CellTag::Duration:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Duration::Primitive>().getMicroseconds());
            return;
        break;

        case CellTag::Point:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Point::Primitive>());
            return;
        break;

        case CellTag::LabelID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<LabelID>().getValue());
            return;
        break;

        case CellTag::EdgeTypeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<EdgeTypeID>().getValue());
            return;
        break;

        case CellTag::PropertyTypeID:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<PropertyTypeID>().getValue());
            return;
        break;

        case CellTag::ValueType:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<ValueType>());
            return;
        break;

        case CellTag::Embedding:
            key.push_back(static_cast<char>(tag));
            distinctAppendValueBytes(key, element.getAs<types::Embedding::Primitive>());
            return;
        break;

        case CellTag::MapView:
            key.push_back(static_cast<char>(tag));
            distinctAppendMapBytes(key, element.getAs<MapView>());
            return;
        break;

        case CellTag::Path:
            key.push_back(static_cast<char>(tag));
            distinctAppendPathBytes(key, element.getAs<PathView>().getIDs());
            return;
        break;

        case CellTag::INVALID:
            throw IRException("cannot dedup by an untagged element");
        break;
    }

    bioassert(false, "Unknown CellTag");
}

// Serialize one row of a type-erased column of tagged scalars into the row key.
void distinctKeyAppendCellColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    distinctAppendElementBytes(key, raw[row]);
}

// Serialize one row of a list column into the row key, so two rows key alike when their
// lists hold equal elements - the equality Cypher gives two lists.
void distinctKeyAppendListColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ListView>*>(column)->getRaw();
    distinctAppendListBytes(key, raw[row]);
}

void distinctKeyAppendMapColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<MapView>*>(column)->getRaw();
    distinctAppendMapBytes(key, raw[row]);
}

void distinctKeyAppendPathColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();
    const GraphPath& path = raw[row];

    if (path.isNull()) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    key.push_back(static_cast<char>(CellTag::Path));
    distinctAppendPathBytes(key, path.getIDs());
}

void distinctKeyAppendOptListColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<ListView>* typedColumn = static_cast<const ColumnOptVector<ListView>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValue(row));
}

void distinctKeyAppendOptMapColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<MapView>* typedColumn = static_cast<const ColumnOptVector<MapView>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back(static_cast<char>(CellTag::Null));
        return;
    }

    distinctAppendPresentValue(key, typedColumn->getValue(row));
}

// Append the values of the given rows of an input chunk onto the tail of a key
// buffer of the same element type. nl.group_aggregate_update passes the rows that
// created a new group this step, so the buffer grows one key value per group in
// creation order.
template <typename ElementType>
void groupGatherAppendColumn(const Column* input,
                             const std::vector<size_t>& rows,
                             Column* buffer,
                             NLValueStore& values) {
    const auto& inputRaw = static_cast<const ColumnVector<ElementType>*>(input)->getRaw();
    auto& bufferRaw = static_cast<ColumnVector<ElementType>*>(buffer)->getRaw();

    for (const size_t row : rows) {
        bufferRaw.push_back(values.ownElement(inputRaw[row]));
    }
}

template <typename Element>
void groupGatherAppendOptColumn(const Column* input,
                                const std::vector<size_t>& rows,
                                Column* buffer,
                                NLValueStore& values) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);
    const std::vector<Element>& inputValues = typedInput->getValues();

    if (typedInput->getNullCount() == 0) {
        for (const size_t row : rows) {
            typedBuffer->push_back(values.ownElement(inputValues[row]));
        }

        return;
    }

    for (const size_t row : rows) {
        if (typedInput->isValid(row)) {
            typedBuffer->push_back(values.ownElement(inputValues[row]));
        } else {
            typedBuffer->pushNull();
        }
    }
}

// A merge keys the value a row asks for against the value the graph holds, and those
// arrive in a plain and a nullable column respectively: the two have to serialize alike,
// which is why a plain row keys its value as a nullable column keys a present one.
template <typename ElementType>
void mergeKeyAppendPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    distinctAppendPresentValue(key, raw[row]);
}

template <typename ElementType>
void mergeKeyAppendConstColumn(const Column* column, size_t row, NLSerializedKey& key) {
    distinctAppendPresentValue(key, (*static_cast<const ColumnConst<ElementType>*>(column))[row]);
}

// The numeric siblings of the three above, keying the row's value in KeyType - the type
// the schema holds the property as - rather than in the one its column holds. The
// analyzer lets an integer constrain a double-typed property, and the graph side keys
// the double it reads back, so the two only compare equal once the row's value is
// converted to it.
template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericPlainColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    distinctAppendPresentValue(key, static_cast<KeyType>(raw[row]));
}

template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericConstColumn(const Column* column, size_t row, NLSerializedKey& key) {
    distinctAppendPresentValue(key, static_cast<KeyType>((*static_cast<const ColumnConst<ElementType>*>(column))[row]));
}

template <typename ElementType, typename KeyType>
void mergeKeyAppendNumericOptColumn(const Column* column, size_t row, NLSerializedKey& key) {
    const ColumnOptVector<ElementType>* typedColumn = static_cast<const ColumnOptVector<ElementType>*>(column);

    if (!typedColumn->isValid(row)) {
        key.push_back('\0');
        return;
    }

    distinctAppendPresentValue(key, static_cast<KeyType>(typedColumn->getValue(row)));
}

// Keys the way a nullable column's null row does, so a merge on a null value binds the
// nodes that lack the property
void mergeKeyAppendConstNull(const Column* column, size_t row, NLSerializedKey& key) {
    key.push_back('\0');
}

// Which of the three shapes a merge property value column comes in, so one selector
// serves all of them
enum class MergeColumnShape {
    Plain,
    Const,
    Opt,
};

template <typename ElementType, typename KeyType, MergeColumnShape Shape>
NLKeyAppendFunction mergeKeyAppendFor() {
    if constexpr (Shape == MergeColumnShape::Plain) {
        return &mergeKeyAppendNumericPlainColumn<ElementType, KeyType>;
    } else if constexpr (Shape == MergeColumnShape::Const) {
        return &mergeKeyAppendNumericConstColumn<ElementType, KeyType>;
    } else {
        return &mergeKeyAppendNumericOptColumn<ElementType, KeyType>;
    }
}

// The appender for a numeric value column, keying its rows in the type the schema holds
// the property as. Identical types convert for free, so one path covers both.
template <typename ElementType, MergeColumnShape Shape>
NLKeyAppendFunction selectNumericMergeKeyAppend(ValueType keyType) {
    switch (keyType) {
        case ValueType::Int64:
            return mergeKeyAppendFor<ElementType, types::Int64::Primitive, Shape>();
        break;

        case ValueType::UInt64:
            return mergeKeyAppendFor<ElementType, types::UInt64::Primitive, Shape>();
        break;

        case ValueType::Double:
            return mergeKeyAppendFor<ElementType, types::Double::Primitive, Shape>();
        break;

        default:
            throw IRException("a MERGE pattern cannot constrain a non-numeric property to a number");
        break;
    }
}

// A non-numeric value converts to no other type the schema might hold the property as,
// so it can only key against a property of its own type
void throwUnlessKeyedAsItsOwnType(ValueType valueType, ValueType keyType) {
    if (valueType != keyType) {
        throw IRException("a MERGE pattern cannot constrain a property to a value of another type");
    }
}

}

NLKeyAppendFunction NLExecutor::selectCellKeyAppendFunction() {
    return &distinctKeyAppendCellColumn;
}

NLGroupKeyGatherFunction NLExecutor::selectCellGroupKeyGatherFunction() {
    return &groupGatherAppendColumn<CellView>;
}

NLKeyAppendFunction NLExecutor::selectOptListKeyAppendFunction() {
    return &distinctKeyAppendOptListColumn;
}

NLGroupKeyGatherFunction NLExecutor::selectOptListGroupKeyGatherFunction() {
    return &groupGatherAppendOptColumn<ListView>;
}

NLGroupKeyGatherFunction NLExecutor::selectOptGroupKeyGather(NLChunkKind kind) {
    NLGroupKeyGatherFunction gather = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { gather = &groupGatherAppendOptColumn<Element>; });

    return gather;
}

NLKeyAppendFunction NLExecutor::selectKeyAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &distinctKeyAppendColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &distinctKeyAppendColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &distinctKeyAppendColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelSetID:
            return &distinctKeyAppendColumn<LabelSetID>;
        break;

        case NLChunkKind::LabelID:
            return &distinctKeyAppendColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &distinctKeyAppendColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &distinctKeyAppendPlainColumn<ValueType>;
        break;

        case NLChunkKind::UInt64:
            return &distinctKeyAppendPlainColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Int64:
            return &distinctKeyAppendPlainColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &distinctKeyAppendPlainColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &distinctKeyAppendPlainColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &distinctKeyAppendPlainColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &distinctKeyAppendPlainColumn<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &distinctKeyAppendListColumn;
        break;

        case NLChunkKind::Map:
            return &distinctKeyAppendMapColumn;
        break;

        case NLChunkKind::Path:
            return &distinctKeyAppendPathColumn;
        break;

        case NLChunkKind::DateTime:
            return &distinctKeyAppendPlainColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &distinctKeyAppendPlainColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &distinctKeyAppendPlainColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a DISTINCT or grouping key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectOptKeyAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &distinctKeyAppendOptColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &distinctKeyAppendOptColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &distinctKeyAppendOptColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &distinctKeyAppendOptColumn<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &distinctKeyAppendOptColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &distinctKeyAppendOptColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &distinctKeyAppendOptColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &distinctKeyAppendOptColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &distinctKeyAppendOptColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            return &distinctKeyAppendOptColumn<types::Embedding::Primitive>;
        break;

        case NLChunkKind::List:
            return &distinctKeyAppendOptListColumn;
        break;

        case NLChunkKind::Map:
            return &distinctKeyAppendOptMapColumn;
        break;

        case NLChunkKind::DateTime:
            return &distinctKeyAppendOptColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &distinctKeyAppendOptColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &distinctKeyAppendOptColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyPacking NLExecutor::selectKeyPacking(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return keyPacking<NodeID>();
        break;

        case NLChunkKind::EdgeID:
            return keyPacking<EdgeID>();
        break;

        case NLChunkKind::EdgeTypeID:
            return keyPacking<EdgeTypeID>();
        break;

        case NLChunkKind::LabelSetID:
            return keyPacking<LabelSetID>();
        break;

        case NLChunkKind::LabelID:
            return keyPacking<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return keyPacking<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return keyPacking<ValueType>();
        break;

        case NLChunkKind::UInt64:
            return keyPacking<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Int64:
            return keyPacking<types::Int64::Primitive>();
        break;

        case NLChunkKind::Double:
            return keyPacking<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return keyPacking<types::Bool::Primitive>();
        break;

        case NLChunkKind::DateTime:
            return keyPacking<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return keyPacking<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return keyPacking<types::Point::Primitive>();
        break;

        case NLChunkKind::String:
        case NLChunkKind::Embedding:
        case NLChunkKind::List:
        case NLChunkKind::Map:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            return NLKeyPacking {};
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return NLKeyPacking {};
}

NLKeyPacking NLExecutor::selectOptKeyPacking(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return optKeyPacking<EdgeTypeID>();
        break;

        case NLChunkKind::LabelID:
            return optKeyPacking<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return optKeyPacking<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return optKeyPacking<ValueType>();
        break;

        case NLChunkKind::Int64:
            return optKeyPacking<types::Int64::Primitive>();
        break;

        case NLChunkKind::UInt64:
            return optKeyPacking<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Double:
            return optKeyPacking<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return optKeyPacking<types::Bool::Primitive>();
        break;

        case NLChunkKind::DateTime:
            return optKeyPacking<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return optKeyPacking<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return optKeyPacking<types::Point::Primitive>();
        break;

        case NLChunkKind::String:
        case NLChunkKind::Embedding:
        case NLChunkKind::List:
        case NLChunkKind::Map:
            return NLKeyPacking {};
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return NLKeyPacking {};
}

NLGroupKeyGatherFunction NLExecutor::selectGroupKeyGather(NLChunkKind kind) {
    NLGroupKeyGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &groupGatherAppendColumn<ElementType>; });

    return selected;
}

NLKeyAppendFunction NLExecutor::selectPlainMergeKeyAppendFunction(NLChunkKind kind, ValueType keyType) {
    switch (kind) {
        case NLChunkKind::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Plain>(keyType);
        break;

        case NLChunkKind::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return &mergeKeyAppendPlainColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return &mergeKeyAppendPlainColumn<types::String::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::EdgeTypeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::LabelID:
        case NLChunkKind::PropertyTypeID:
        case NLChunkKind::ValueTypeCode:
        case NLChunkKind::Embedding:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("a MERGE pattern cannot constrain a property to this value");
        break;

        case NLChunkKind::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return &mergeKeyAppendPlainColumn<ListView>;
        break;

        case NLChunkKind::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return &mergeKeyAppendPlainColumn<MapView>;
        break;

        case NLChunkKind::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return &mergeKeyAppendPlainColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return &mergeKeyAppendPlainColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return &mergeKeyAppendPlainColumn<types::Point::Primitive>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectConstMergeKeyAppendFunction(ValueType valueType, ValueType keyType) {
    switch (valueType) {
        case ValueType::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Const>(keyType);
        break;

        case ValueType::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return &mergeKeyAppendConstColumn<types::Bool::Primitive>;
        break;

        case ValueType::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return &mergeKeyAppendConstColumn<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throwUnlessKeyedAsItsOwnType(ValueType::Embedding, keyType);
            return &mergeKeyAppendConstColumn<types::Embedding::Primitive>;
        break;

        case ValueType::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return &mergeKeyAppendConstColumn<ListView>;
        break;

        case ValueType::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return &mergeKeyAppendConstColumn<types::DateTime::Primitive>;
        break;

        case ValueType::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return &mergeKeyAppendConstColumn<types::Duration::Primitive>;
        break;

        case ValueType::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return &mergeKeyAppendConstColumn<types::Point::Primitive>;
        break;

        case ValueType::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return &mergeKeyAppendConstColumn<MapView>;
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid MERGE property value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectOptMergeKeyAppendFunction(NLChunkKind kind, ValueType keyType) {
    switch (kind) {
        case NLChunkKind::Int64:
            return selectNumericMergeKeyAppend<types::Int64::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::UInt64:
            return selectNumericMergeKeyAppend<types::UInt64::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::Double:
            return selectNumericMergeKeyAppend<types::Double::Primitive, MergeColumnShape::Opt>(keyType);
        break;

        case NLChunkKind::Bool:
            throwUnlessKeyedAsItsOwnType(ValueType::Bool, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::String:
            throwUnlessKeyedAsItsOwnType(ValueType::String, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Embedding:
            throwUnlessKeyedAsItsOwnType(ValueType::Embedding, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::List:
            throwUnlessKeyedAsItsOwnType(ValueType::List, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Map:
            throwUnlessKeyedAsItsOwnType(ValueType::Map, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::DateTime:
            throwUnlessKeyedAsItsOwnType(ValueType::DateTime, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Duration:
            throwUnlessKeyedAsItsOwnType(ValueType::Duration, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::Point:
            throwUnlessKeyedAsItsOwnType(ValueType::Point, keyType);
            return selectOptKeyAppendFunction(kind);
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::EdgeTypeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::LabelID:
        case NLChunkKind::PropertyTypeID:
        case NLChunkKind::ValueTypeCode:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("a MERGE pattern cannot constrain a property to this value");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLKeyAppendFunction NLExecutor::selectNullMergeKeyAppendFunction() {
    return &mergeKeyAppendConstNull;
}
