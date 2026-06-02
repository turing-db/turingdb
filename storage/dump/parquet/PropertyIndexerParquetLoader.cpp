#include "PropertyIndexerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <optional>
#include <span>
#include <vector>

#include "ParquetReader.h"

#include "indexers/PropertyIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "metadata/LabelSetMap.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

// One row per range: four INT64 columns (property_type_id, labelset_id, offset, count).
class PropertyIndexerVisitor : public ParquetSaxVisitor {
public:
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _labelsetIds;
    std::vector<int64_t> _offsets;
    std::vector<int64_t> _counts;

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = columnFor(columnIndex);
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    std::vector<int64_t>& columnFor(size_t columnIndex) {
        if (columnIndex == 0) {
            return _propertyTypeIds;
        } else if (columnIndex == 1) {
            return _labelsetIds;
        } else if (columnIndex == 2) {
            return _offsets;
        } else {
            return _counts;
        }
    }
};

}

void PropertyIndexerParquetLoader::load(const fs::Path& path,
                                        const LabelSetMap& labelsets,
                                        PropertyIndexer& out) {
    PropertyIndexerVisitor visitor;
    {
        ParquetReader reader(path, visitor);
        while (reader.nextChunk()) {
        }
    }

    for (size_t i = 0; i < visitor._propertyTypeIds.size(); ++i) {
        const PropertyTypeID propertyTypeID {
            static_cast<PropertyTypeID::Type>(visitor._propertyTypeIds[i])};

        const std::optional<LabelSetHandle> handle =
            labelsets.getValue(static_cast<LabelSetID::Type>(visitor._labelsetIds[i]));
        if (!handle) {
            throw FatalException("PropertyIndexerParquetLoader: unknown labelset id");
        }

        const PropertyRange range {
            static_cast<size_t>(visitor._offsets[i]),
            static_cast<size_t>(visitor._counts[i])};

        out[propertyTypeID][handle.value()].push_back(range);
    }
}
