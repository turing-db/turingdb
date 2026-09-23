#pragma once

#include <stddef.h>

#include "MapView.h"

namespace db {

/// Hashes a map by its keys and values, so two maps Cypher's = calls equal hash alike
size_t hashMap(MapView map);

/// True when @param lhs and @param rhs hold the same keys with the same tagged values,
/// compared bit for bit: stricter than Cypher's =, so 1 and 1.0 differ here
bool sameMap(MapView lhs, MapView rhs);

}
