#pragma once

#include <memory>
#include <string_view>

#include "ID.h"
#include "Path.h"

namespace db {

class DataPart;
class LabelSetMap;
class PropertyManager;
class PropertyContainer;

// Rebuilds a whole DataPart from the directory written by DataPartParquetDumper,
// delegating to each per-structure loader and assembling the result through friend
// access (as DataPartLoader does). Label-set keys and edge spans are bound via the
// supplied LabelSetMap and the loaded EdgeContainer; nothing is reconstructed.
// Throws on failure.
class DataPartParquetLoader {
public:
    static std::unique_ptr<DataPart> load(const fs::Path& partDir, const LabelSetMap& labelsets);

private:
    static void loadPropertyManager(const fs::Path& partDir,
                                    const LabelSetMap& labelsets,
                                    const fs::Path& indexerPath,
                                    std::string_view propsPrefix,
                                    PropertyManager& manager);

    static void insertContainer(PropertyManager& manager,
                                PropertyTypeID propertyTypeID,
                                std::unique_ptr<PropertyContainer> container);
};

}
