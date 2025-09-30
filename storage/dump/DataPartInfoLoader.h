#pragma once

#include "FilePageReader.h"
#include "GraphDumpHelper.h"
#include "DataPart.h"

namespace db {

class DataPartInfoLoader {
public:
    explicit DataPartInfoLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(DataPart& part) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_DATAPART_INFO, _reader.error().value());
        }

        // Start reading metadata page
        auto it = _reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_DATAPART_INFO);
        }

        if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
            return res.get_unexpected();
        }

        [[maybe_unused]] const uint64_t nodeCount = it.get<uint64_t>();
        [[maybe_unused]] const uint64_t edgeCount = it.get<uint64_t>();
        const NodeID firstNodeID = it.get<uint64_t>();
        const EdgeID firstEdgeID = it.get<uint64_t>();

        part._firstNodeID = firstNodeID;
        part._firstEdgeID = firstEdgeID;

        return {};
    }

private:
    fs::FilePageReader& _reader;
};

}
