#pragma once

#include <stddef.h>

#include <optional>
#include <span>
#include <string>
#include <vector>

#include "CellTag.h"
#include "map/MapView.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

class Column;
class GraphView;
class MetadataBuilder;

// Where this change's provisional IDs start. A node or edge it writes is named by the ID
// it will commit as - one past the last the graph holds, plus the entity's offset in the
// write buffer - so these are what turn one into the other.
size_t committedNodeCount(const GraphView* view);
size_t committedEdgeCount(const GraphView* view);

bool holdsValue(const CommitWriteBuffer::SupportedTypeVariant& value);

// The values one property column holds, as the write buffer takes them: one entry per
// row, owning whatever the column only borrows. What a create, a set and a merge all
// turn a row's asked-for value into before writing it.
void extractColumnProperties(const Column* column,
                             size_t rowCount,
                             PropertyTypeID propID,
                             CommitWriteBuffer::UntypedProperties& buf);

// The values a column of cells - list elements or map values - holds, as a property of
// @param valueType takes them. Each cell carries its own type, so a cell holding another
// type throws on its row.
void extractCellProperties(const Column* column,
                           size_t rowCount,
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

// The property type one key of the map staged last resolved to. Keys are stored sorted, so
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

// Appends the properties a map names, each staged as its property's type takes it. A key no
// property carries throws, since a map value's type is only known per row, unless its
// value is null, which removes nothing.
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

// The disengaged value of one property, repeated over every row. A write of a null has no
// value column to read a type off, so the property's own type picks the variant it stages.
void fillNullProperties(size_t rowCount,
                        PropertyTypeID propID,
                        ValueType valueType,
                        CommitWriteBuffer::UntypedProperties& buf);

}
