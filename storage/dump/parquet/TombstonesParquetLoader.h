#pragma once

#include "Path.h"

namespace db {

class Tombstones;

// Rebuilds a Tombstones' node and edge sets from the files written by
// TombstonesParquetDumper, inserting the ids into the (unordered) sets. The output
// Tombstones is expected to be empty. Friend of Tombstones to reach its private mutable
// sets. Throws on failure (I/O, decode).
class TombstonesParquetLoader {
public:
    static void load(const fs::Path& commitDir, Tombstones& out);
};

}
