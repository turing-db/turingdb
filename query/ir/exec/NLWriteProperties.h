#pragma once

#include <stddef.h>

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "CellTag.h"
#include "map/MapView.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

class Column;

template <typename T>
class ColumnVector;

class GraphView;
class MetadataBuilder;

bool holdsValue(const CommitWriteBuffer::SupportedTypeVariant& value);

std::optional<PropertyType> findWrittenPropertyType(const GraphView* view,
                                                    const MetadataBuilder* metadataBuilder,
                                                    std::string_view name);
std::optional<PropertyType> findWrittenPropertyType(const GraphView* view,
                                                    const MetadataBuilder* metadataBuilder,
                                                    PropertyTypeID id);

// A write with no rows mask touches every row
bool writeTouchesRow(const ColumnVector<CustomBool>* rows, size_t row);

// The values one property column holds, as the write buffer takes them: one entry per
// row, owning whatever the column only borrows. What a create, a set and a merge all
// turn a row's asked-for value into before writing it.
void extractColumnProperties(const Column* column,
                             size_t rowCount,
                             PropertyTypeID propID,
                             CommitWriteBuffer::UntypedProperties& buf);

// The values a column of cells - list elements or map values - holds, as a property of
// @param valueType takes them. Each cell carries its own type, so a cell holding another
// type throws on its row. @param written holds the rows the write reaches, or is null for
// every row: a row it does not reach is neither checked nor staged.
void extractCellProperties(const Column* column,
                           size_t rowCount,
                           const ColumnVector<CustomBool>* written,
                           PropertyTypeID propID,
                           ValueType valueType,
                           CommitWriteBuffer::UntypedProperties& buf);

// The map one row writes, absent with the tag Null for a row holding null. A cell holding
// another value keeps its tag, so the error is raised only for a row the write touches.
struct WrittenMap {
    std::optional<MapView> _map;
    CellTag _tag {CellTag::MapView};
};

void extractWrittenMaps(const Column* column,
                        size_t rowCount,
                        std::vector<WrittenMap>& maps);

// The property type one key of the map read last resolved to. Keys are stored sorted, so
// the maps a column builds per row line their keys up and resolve each name once.
struct MapKeyPropertyType {
    std::string _key;
    std::optional<PropertyType> _type;
    bool _resolved {false};
};

struct StagedRange {
    size_t _begin {0};
    size_t _end {0};
};

// The type a key no property carries takes from the values the maps of one write give it
struct MapKeyCreatedType {
    std::string _key;
    ValueType _type {ValueType::Invalid};
};

// A key no property carried may name one a write has created since
void forgetUntypedMapKeys(std::vector<MapKeyPropertyType>& keyTypes);

// Widens the type of each key of @param map that no property carries by its value, as a
// write of cells types a new property. A null value leaves the type as it is.
void widenCreatedMapKeyTypes(const WrittenMap& map,
                             const GraphView* view,
                             const MetadataBuilder* metadataBuilder,
                             std::vector<MapKeyPropertyType>& keyTypes,
                             std::vector<MapKeyCreatedType>& created);

// Appends the properties a map names, each staged as its property's type takes it. A key no
// property carries is skipped where its value is null, which removes nothing; any other
// names a property created from the type widenCreatedMapKeyTypes widened, and @param
// keyTypes has forgotten its untyped keys since.
void stageMapProperties(const WrittenMap& map,
                        const GraphView* view,
                        const MetadataBuilder* metadataBuilder,
                        std::vector<MapKeyPropertyType>& keyTypes,
                        CommitWriteBuffer::UntypedProperties& staged);

// A removal of each held property that @param written does not write. @param heldTypes
// keeps the value type of each property ID it has looked up.
void stageRemovals(std::span<const PropertyTypeID> held,
                   std::span<const CommitWriteBuffer::UntypedProperty> written,
                   const GraphView* view,
                   const MetadataBuilder* metadataBuilder,
                   std::vector<ValueType>& heldTypes,
                   CommitWriteBuffer::UntypedProperties& removals);

// The type a property no write has typed takes from the @param rowCount rows of a column of
// tagged cells @param written holds: the one every value they hold is staged as. Invalid for
// no row or rows of nulls. Raises where two values share no such type, or where one is a
// value no property holds.
ValueType cellPropertyType(const Column* column,
                           size_t rowCount,
                           const ColumnVector<CustomBool>* written,
                           std::string_view propertyName);

// The disengaged value of one property, repeated over every row. A write of a null has no
// value column to read a type off, so the property's own type picks the variant it stages.
void fillNullProperties(size_t rowCount,
                        PropertyTypeID propID,
                        ValueType valueType,
                        CommitWriteBuffer::UntypedProperties& buf);

}
