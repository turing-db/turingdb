#pragma once

#include "GraphDumpHelper.h"
#include "DataPart.h"

namespace db {

class DataPartInfoDumper {
public:
    explicit DataPartInfoDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const DataPart& part) {
        GraphDumpHelper::writeFileHeader(_writer);

        const uint64_t nodeCount = part.getNodeCount();
        const uint64_t edgeCount = part.getEdgeCount();
        const NodeID firstNodeID = part.getFirstNodeID();
        const EdgeID firstEdgeID = part.getFirstEdgeID();

        _writer.writeToCurrentPage(nodeCount);
        _writer.writeToCurrentPage(edgeCount);
        _writer.writeToCurrentPage(firstNodeID.getValue());
        _writer.writeToCurrentPage(firstEdgeID.getValue());

        _writer.finish();

        if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_DATAPART_INFO, _writer.error().value());
        }

        return {};
    }

private:
    fs::FilePageWriter& _writer;
};

};
