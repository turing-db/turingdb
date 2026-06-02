#pragma once

#include <memory>

#include "Path.h"

namespace db {

class StringPropertyIndexer;

// Rebuilds a StringPropertyIndexer from the three files written by
// StringPropertyIndexerParquetDumper. Each index is created with its node count
// (StringIndex pre-allocates dense nodes), then child links and owners are applied
// directly — no strings are re-inserted. Owners are plain entity ids, so no graph
// metadata is needed. Throws on failure.
class StringPropertyIndexerParquetLoader {
public:
    static std::unique_ptr<StringPropertyIndexer> load(const fs::Path& indexesPath,
                                                       const fs::Path& childrenPath,
                                                       const fs::Path& ownersPath);
};

}
