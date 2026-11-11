#pragma once

#include "mlir/IR/Types.h"

namespace mlir::storage {

bool isEntityIDType(Type type);

// The types a const scan of nodes or edges reads the IDs bound when the query runs from: an
// integer or an entity of @param entity's kind, the one entity it names, or a list, whose
// numbers are the IDs it lists. Shared by the const scans and their nl siblings.
bool isBoundEntityIDsType(Type type, Type entity);

// The types entity_id_of reads: any value, which names the entity whose ID it equals or none,
// and a node or an edge, which an ID equals by its number. A path has no ID. Shared by
// db.entity_id_of and its nl sibling.
bool isEntityIDOfInputType(Type type);

}
