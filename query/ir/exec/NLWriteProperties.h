#pragma once

#include <stddef.h>

#include "list/ListElementView.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

class Column;
class GraphView;

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

// The disengaged value of one property, repeated over every row. A write of a null has no
// value column to read a type off, so the property's own type picks the variant it stages.
void fillNullProperties(size_t rowCount,
                        PropertyTypeID propID,
                        ValueType valueType,
                        CommitWriteBuffer::UntypedProperties& buf);

}
