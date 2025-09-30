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
    [[nodiscard]] static DumpResult<void> load(const fs::Path& path, GraphMetadata& metadata) {
        // Reading labels
        {
            const fs::Path labelsPath = path / "labels";
            auto reader = fs::FilePageReader::open(labelsPath, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELS, reader.error());
            }

            LabelMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata._labelMap); !res) {
                return res.get_unexpected();
            }
        }

        {
            const fs::Path edgeTypesPath = path / "edge-types";
            auto reader = fs::FilePageReader::open(edgeTypesPath, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_EDGE_TYPES, reader.error());
            }

            EdgeTypeMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata._edgeTypeMap); !res) {
                return res.get_unexpected();
            }
        }

        // Reading property types
        {
            const fs::Path propTypesPath = path / "property-types";
            auto reader = fs::FilePageReader::open(propTypesPath, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, reader.error());
            }

            PropertyTypeMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata._propTypeMap); !res) {
                return res.get_unexpected();
            }
        }

        // Reading labelsets
        {
            const fs::Path labelsetsPath = path / "labelsets";
            auto reader = fs::FilePageReader::open(labelsetsPath, DumpConfig::PAGE_SIZE);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELSETS, reader.error());
            }

            LabelSetMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata._labelsetMap); !res) {
                return res.get_unexpected();
            }
        }

        return {};
    }
};

}
