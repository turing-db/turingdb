#pragma once

#include <memory>

#include "Path.h"

namespace db {

class PropertyContainer;

// Reads a property container written by PropertyContainerParquetDumper back into a
// freshly built TypedPropertyContainer<T>, dispatching on the value type stored in
// the file metadata. Throws on failure (missing/invalid metadata, I/O, decode).
class PropertyContainerParquetLoader {
public:
    static std::unique_ptr<PropertyContainer> load(const fs::Path& path);
};

}
