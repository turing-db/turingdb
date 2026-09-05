#pragma once

#include <stddef.h>

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
// row, owning whatever the column only borrows. What a create, a set and a merge all
// turn a row's asked-for value into before writing it.
void extractColumnProperties(const Column* column,
                             size_t rowCount,
                             PropertyTypeID propID,
                             CommitWriteBuffer::UntypedProperties& buf);

}
