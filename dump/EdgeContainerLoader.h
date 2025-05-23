#pragma once

#include <memory>

#include "FilePageReader.h"
#include "DumpConfig.h"
#include "GraphDumpHelper.h"
#include "EdgeContainer.h"
#include "EdgeContainerDumpConstants.h"
#include "Profiler.h"

namespace db {

class EdgeContainerLoader {
public:
    using Constants = EdgeContainerDumperConstants;

    explicit EdgeContainerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<EdgeContainer>> load() {
        Profile profile {"EdgeContainerLoader::load"};

        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGES,
                                     _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        // Check if we received a full page
        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGES);
        }

        // Check file header
        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        const uint64_t firstEdgeID = it.get<uint64_t>();
        const uint64_t firstNodeID = it.get<uint64_t>();
        const uint64_t edgeCount = it.get<uint64_t>();
        const uint64_t pageCountPerDir = it.get<uint64_t>();

        std::vector<EdgeRecord> outEdges;
        std::vector<EdgeRecord> inEdges;

        outEdges.resize(edgeCount);
        inEdges.resize(edgeCount);

        const auto loadEdges = [&](auto& edges) -> DumpResult<void> {
            size_t recordOffset = 0;
            for (size_t i = 0; i < pageCountPerDir; i++) {
                _reader.nextPage();

                if (_reader.errorOccured()) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGES,
                                             _reader.error().value());
                }

                it = _reader.begin();

                // Check that we read a whole page
                if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                    return DumpError::result(DumpErrorType::COULD_NOT_READ_EDGES);
                }

                const size_t countInPage = it.get<uint64_t>();

                // Copy content of page into edge records vector
                for (size_t j = 0; j < countInPage; j++) {
                    auto& edge = edges[j + recordOffset];
                    edge._edgeID = it.get<EntityID::Type>();
                    edge._nodeID = it.get<EntityID::Type>();
                    edge._otherID = it.get<EntityID::Type>();
                    edge._edgeTypeID = it.get<EdgeTypeID::Type>();
                }

                recordOffset += countInPage;
            }

            return {};
        };

        if (auto res = loadEdges(outEdges); !res) {
            return res.get_unexpected();
        }

        if (auto res = loadEdges(inEdges); !res) {
            return res.get_unexpected();
        }

        EdgeContainer* container = new EdgeContainer {
            firstNodeID,
            firstEdgeID,
            std::move(outEdges),
            std::move(inEdges),
        };

        return {std::unique_ptr<EdgeContainer> {container}};
    }

private:
    fs::FilePageReader& _reader;
};

}
