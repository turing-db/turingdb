#pragma once

#include "FilePageReader.h"
#include "metadata/GraphMetadata.h"
#include "LabelMapLoader.h"
#include "EdgeTypeMapLoader.h"
#include "PropertyTypeMapLoader.h"
#include "LabelSetMapLoader.h"

namespace db {

class GraphMetadata;

class GraphMetadataLoader {
public:
    [[nodiscard]] static DumpResult<void> load(const fs::Path& metaDataDir, GraphMetadata& metadata) {
        // Reading labels
        {
            const fs::Path labelsFile = metaDataDir / "labels";
            auto reader = fs::FilePageReader::open(labelsFile, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELS, reader.error());
            }

            LabelMapLoader loader(reader.value());

            if (auto res = loader.load(metadata._labelMap); !res) {
                return res.get_unexpected();
            }
        }

        {
            const fs::Path edgeTypesFile = metaDataDir / "edge-types";
            auto reader = fs::FilePageReader::open(edgeTypesFile, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_EDGE_TYPES, reader.error());
            }

            EdgeTypeMapLoader loader(reader.value());

            if (auto res = loader.load(metadata._edgeTypeMap); !res) {
                return res.get_unexpected();
            }
        }

        // Reading property types
        {
            const fs::Path propTypesFile = metaDataDir / "property-types";
            auto reader = fs::FilePageReader::open(propTypesFile, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, reader.error());
            }

            PropertyTypeMapLoader loader(reader.value());

            if (auto res = loader.load(metadata._propTypeMap); !res) {
                return res.get_unexpected();
            }
        }

        // Reading labelsets
        {
            const fs::Path labelsetsFile = metaDataDir / "labelsets";
            auto reader = fs::FilePageReader::open(labelsetsFile, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELSETS, reader.error());
            }

            LabelSetMapLoader loader(reader.value());

            if (auto res = loader.load(metadata._labelsetMap); !res) {
                return res.get_unexpected();
            }
        }

        return {};
    }
};
}
