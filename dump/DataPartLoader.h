#pragma once

#include "Path.h"
#include "DumpResult.h"
#include "ArcManager.h"

namespace db {

class DataPart;
class PropertyManager;
class GraphMetadata;
class VersionController;

class DataPartLoader {
public:
    [[nodiscard]] static DumpResult<WeakArc<const DataPart>> load(const fs::Path& path,
                                                                  const GraphMetadata& metadata,
                                                                  VersionController& versionController);

private:
    static constexpr std::string_view NODE_PROPS_PREFIX = "node-props-";
    static constexpr std::string_view EDGE_PROPS_PREFIX = "edge-props-";
    static constexpr size_t PREFIX_SIZE = NODE_PROPS_PREFIX.size();
    static_assert(PREFIX_SIZE == EDGE_PROPS_PREFIX.size());
};

}
