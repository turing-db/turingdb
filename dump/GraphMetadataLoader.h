#pragma once

#include "FilePageReader.h"
#include "GraphMetadata.h"
#include "LabelMapLoader.h"
#include "EdgeTypeMapLoader.h"
#include "PropertyTypeMapLoader.h"
#include "LabelSetMapLoader.h"

namespace db {

class GraphMetadata;

class GraphMetadataLoader {
public:
    [[nodiscard]] static DumpResult<std::unique_ptr<GraphMetadata>> load(const fs::Path& path) {
        auto metadata = std::make_unique<GraphMetadata>();

        // Reading labels
        {
            const fs::Path labelsPath = path / "labels";
            auto reader = fs::FilePageReader::open(labelsPath);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELS, reader.error());
            }

            LabelMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata->labels()); !res) {
                return res.get_unexpected();
            }
        }

        {
            const fs::Path edgeTypesPath = path / "edge-types";
            auto reader = fs::FilePageReader::open(edgeTypesPath);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_EDGE_TYPES, reader.error());
            }

            EdgeTypeMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata->edgeTypes()); !res) {
                return res.get_unexpected();
            }
        }

        // Reading property types
        {
            const fs::Path propTypesPath = path / "property-types";
            auto reader = fs::FilePageReader::open(propTypesPath);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_PROPERTY_TYPES, reader.error());
            }

            PropertyTypeMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata->propTypes()); !res) {
                return res.get_unexpected();
            }
        }

        // Reading labelsets
        {
            const fs::Path labelsetsPath = path / "labelsets";
            auto reader = fs::FilePageReader::open(labelsetsPath);
            if (!reader) {
                return DumpError::result(DumpErrorType::CANNOT_OPEN_LABELSETS, reader.error());
            }

            LabelSetMapLoader loader {reader.value()};

            if (auto res = loader.load(metadata->labelsets()); !res) {
                return res.get_unexpected();
            }
        }

        return std::move(metadata);
    }
};

}
