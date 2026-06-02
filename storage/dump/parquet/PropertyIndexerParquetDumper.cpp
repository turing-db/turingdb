#include "PropertyIndexerParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <string_view>
#include <vector>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "indexers/PropertyIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "metadata/LabelSetHandle.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

constexpr std::string_view PROPERTY_TYPE_ID_COLUMN = "property_type_id";
constexpr std::string_view LABELSET_ID_COLUMN = "labelset_id";
constexpr std::string_view OFFSET_COLUMN = "offset";
constexpr std::string_view COUNT_COLUMN = "count";

}

void PropertyIndexerParquetDumper::dump(const PropertyIndexer& indexer, const fs::Path& path) {
    size_t totalRanges = 0;
    for (const auto& [propertyTypeID, labelSetIndexer] : indexer) {
        if (labelSetIndexer.size() == 0) {
            throw FatalException("PropertyIndexerParquetDumper: property type has no label sets");
        }
        for (const auto& [labelset, ranges] : labelSetIndexer) {
            if (ranges.size() == 0) {
                throw FatalException("PropertyIndexerParquetDumper: label set has no ranges");
            }
            totalRanges += ranges.size();
        }
    }

    ParquetWriteSchema schema;
    schema.addColumn(PROPERTY_TYPE_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(LABELSET_ID_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(OFFSET_COLUMN, ParquetColumnType::UInt64);
    schema.addColumn(COUNT_COLUMN, ParquetColumnType::UInt64);

    ParquetWriter writer(path, schema);

    if (totalRanges > 0) {
        std::vector<int64_t> propertyTypeIds;
        std::vector<int64_t> labelsetIds;
        std::vector<int64_t> offsets;
        std::vector<int64_t> counts;
        propertyTypeIds.reserve(totalRanges);
        labelsetIds.reserve(totalRanges);
        offsets.reserve(totalRanges);
        counts.reserve(totalRanges);

        for (const auto& [propertyTypeID, labelSetIndexer] : indexer) {
            for (const auto& [labelset, ranges] : labelSetIndexer) {
                for (const PropertyRange& range : ranges) {
                    propertyTypeIds.push_back(static_cast<int64_t>(propertyTypeID.getValue()));
                    labelsetIds.push_back(static_cast<int64_t>(labelset.getID().getValue()));
                    offsets.push_back(static_cast<int64_t>(range._offset));
                    counts.push_back(static_cast<int64_t>(range._count));
                }
            }
        }

        writer.beginRowGroup(totalRanges);
        writer.writeInt64Column(0, propertyTypeIds);
        writer.writeInt64Column(1, labelsetIds);
        writer.writeInt64Column(2, offsets);
        writer.writeInt64Column(3, counts);
    }

    writer.finish();
}
