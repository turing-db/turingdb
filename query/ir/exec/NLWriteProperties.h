#pragma once

#include <stddef.h>

#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "llvm/ADT/STLFunctionalExtras.h"

#include "list/ListElementView.h"
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

// The values one property column holds, as the write buffer takes them: one entry per
// row, owning whatever the column only borrows, and of the property's own type. What a
// create, a set and a merge all turn a row's asked-for value into before writing it.
void extractColumnProperties(const Column* column,
                             size_t rowCount,
                             PropertyType property,
                             CommitWriteBuffer::UntypedProperties& buf);

// Whether a column holds one type-tagged cell per row
bool readsTaggedCells(const Column* column);

// The tagged cell one row of a column of them holds: a null cell where the row holds none
ListElementView taggedCellAt(const Column* column, size_t row);

// The type of property a tagged cell's value makes, Invalid for a null. Cypher has one
// integer type, and it is signed.
ValueType taggedCellValueType(ListElementView cell);

// One tagged cell as the write buffer stages it for a property of @param valueType. A cell
// carries its type per row, so one no property of that type can hold throws here.
void stageTaggedCell(ListElementView cell,
                     ValueType valueType,
                     CommitWriteBuffer::SupportedTypeVariant& staged);

// The tagged cells of the rows @param stagesRow keeps, staged for @param property. The cell
// of a row it skips - one an OPTIONAL MATCH found no entity for - is neither checked nor read.
void stageTaggedCells(const Column* column,
                      size_t rowCount,
                      PropertyType property,
                      llvm::function_ref<bool(size_t)> stagesRow,
                      CommitWriteBuffer::UntypedProperties& buf);

// The property tagged cells write under a name the graph did not have at translation: the
// one a write registered under it since, else a new one typed by the first cell holding a
// value. Invalid while no cell the write stages holds one, as there is nothing to type it by.
PropertyType resolveTaggedCellProperty(MetadataBuilder* metadataBuilder,
                                       std::string_view name,
                                       const Column* column,
                                       size_t rowCount,
                                       llvm::function_ref<bool(size_t)> stagesRow);

// The disengaged value of one property, repeated over every row. A write of a null has no
// value column to read a type off, so the property's own type picks the variant it stages.
void fillNullProperties(size_t rowCount,
                        PropertyTypeID propID,
                        ValueType valueType,
                        CommitWriteBuffer::UntypedProperties& buf);

// The map one row of a column of them holds: none where the row holds a null
std::optional<MapView> mapCellAt(const Column* column, size_t row);

// The properties one map writes, each entry staged under the property its key names. A key
// the change does not know yet makes a property of its entry's type, appended to
// @param created. A null entry removes its property.
void stageMapEntries(MapView map,
                     MetadataBuilder* metadataBuilder,
                     std::vector<PropertyType>& created,
                     CommitWriteBuffer::UntypedProperties& staged);

// A null for each property of @param known that no entry of @param staged sets: what
// SET n = m writes for the properties m does not hold
void stageMapRemovals(std::span<const PropertyType> known, CommitWriteBuffer::UntypedProperties& staged);

}
