#pragma once

#include <memory>

#include "FilePageReader.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "NodeContainer.h"
#include "Profiler.h"
#include "metadata/GraphMetadata.h"
#include "NodeContainerDumpConstants.h"

namespace db {

class NodeContainerLoader {
public:
    using Constants = NodeContainerDumperConstants;

    explicit NodeContainerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<NodeContainer>> load(const GraphMetadata& metadata) {
        Profile profile {"NodeContainerLoader::load"};

        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        // Check if we received a full page
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
        }

        // Check file header
        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const uint64_t firstID = it.get<uint64_t>();
        const uint64_t nodeCount = it.get<uint64_t>();
        [[maybe_unused]] const uint64_t rangeCount = it.get<uint64_t>();
        const uint64_t recordPageCount = it.get<uint64_t>();
        const uint64_t rangePageCount = it.get<uint64_t>();

        NodeContainer* container = new NodeContainer {firstID, nodeCount};

        auto& ranges = container->_ranges;
        auto& nodes = container->_nodes;
        nodes.resize(nodeCount);

        // Loading node ranges
        for (size_t i = 0; i < rangePageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
            }

            const size_t countInPage = it.get<uint64_t>();

            for (size_t j = 0; j < countInPage; j++) {
                const auto lsetID = it.get<LabelSetID::Type>();
                const auto& lset = metadata.labelsets().getValue(lsetID);

                if (!lset) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
                }

                auto& r = ranges[lset.value()];
                r._first = it.get<EntityID::Type>();
                r._count = it.get<uint64_t>();
            }
        }

        // loading node records
        size_t recordOffset = 0;
        for (size_t i = 0; i < recordPageCount; i++) {
            _reader.nextPage();

            if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES, _reader.error().value());
            }

            it = _reader.begin();

            // Check that we read a whole page
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
            }

            const size_t countInPage = it.get<uint64_t>();

            // Copy content of page into node records vector
            for (size_t j = 0; j < countInPage; j++) {
                const LabelSetID labelsetID = it.get<LabelSetID::Type>();
                const auto labelset = metadata.labelsets().getValue(labelsetID);

                if (!labelset) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_NODES);
                }

                nodes[j + recordOffset]._labelset = labelset.value();
            }

            recordOffset += countInPage;
        }

        return {std::unique_ptr<NodeContainer> {container}};
    }

private:
    fs::FilePageReader& _reader;
};

}
