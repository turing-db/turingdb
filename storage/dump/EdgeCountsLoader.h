#pragma once

#include "DumpResult.h"

namespace fs {
class FilePageReader;
}

namespace db {

class EdgeCounts;
class GraphMetadata;

class EdgeCountsLoader {
public:
    explicit EdgeCountsLoader(fs::FilePageReader& reader);
    ~EdgeCountsLoader();

    [[nodiscard]] DumpResult<void> load(const GraphMetadata& metadata, EdgeCounts& counts);

private:
    fs::FilePageReader& _reader;
};

}
