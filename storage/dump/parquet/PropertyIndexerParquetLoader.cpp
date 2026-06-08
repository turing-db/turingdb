#include "PropertyIndexerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

#include "PropertyIndexerParquetLayout.h"

#include "indexers/PropertyIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "metadata/LabelSetMap.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = propertyIndexerParquetLayout;

namespace {

// One row per range: four INT64 columns (property_type_id, labelset_id, offset, count).
struct PropertyIndexerData {
    std::vector<int64_t> _propertyTypeIds;
    std::vector<int64_t> _labelsetIds;
    std::vector<int64_t> _offsets;
    std::vector<int64_t> _counts;
};

// Fills the caller-owned PropertyIndexerData, routing each column by its index.
class PropertyIndexerVisitor : public ParquetSaxVisitor {
public:
    explicit PropertyIndexerVisitor(PropertyIndexerData& data)
        : _data(data) {
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = columnFor(columnIndex);
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

private:
    PropertyIndexerData& _data;

    std::vector<int64_t>& columnFor(size_t columnIndex) {
        if (columnIndex == 0) {
            return _data._propertyTypeIds;
        } else if (columnIndex == 1) {
            return _data._labelsetIds;
        } else if (columnIndex == 2) {
            return _data._offsets;
        } else if (columnIndex == 3) {
            return _data._counts;
        } else {
            throw FatalException("PropertyIndexerParquetLoader: unexpected column");
        }
    }
};

}

void PropertyIndexerParquetLoader::load(const fs::Path& path,
                                        const LabelSetMap& labelsets,
                                        PropertyIndexer& out) {
    PropertyIndexerData data;
    {
        PropertyIndexerVisitor visitor(data);

        ParquetWriteSchema expectedSchema;
        expectedSchema.addColumn(layout::PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::OFFSET_COLUMN, ParquetColumnType::UInt64);
        expectedSchema.addColumn(layout::COUNT_COLUMN, ParquetColumnType::UInt64);

        ParquetReader reader(path, visitor);
        reader.setExpectedSchema(expectedSchema);
        while (reader.nextChunk()) {
        }
    }

    const bool columnsAgree = data._labelsetIds.size() == data._propertyTypeIds.size()
                              && data._offsets.size() == data._propertyTypeIds.size()
                              && data._counts.size() == data._propertyTypeIds.size();
    if (!columnsAgree) {
        throw FatalException("PropertyIndexerParquetLoader: columns have mismatched lengths");
    }

    for (size_t i = 0; i < data._propertyTypeIds.size(); ++i) {
        const PropertyTypeID propertyTypeID {
            static_cast<PropertyTypeID::Type>(data._propertyTypeIds[i])};

        const std::optional<LabelSetHandle> handle =
            labelsets.getValue(static_cast<LabelSetID::Type>(data._labelsetIds[i]));
        if (!handle) {
            throw FatalException("PropertyIndexerParquetLoader: unknown labelset id");
        }

        const PropertyRange range {
            static_cast<size_t>(data._offsets[i]),
            static_cast<size_t>(data._counts[i])};

        out[propertyTypeID][handle.value()].push_back(range);
    }
}
