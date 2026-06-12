#include "NLProgram.h"

#include "BioAssert.h"

using namespace db;

NLProgram::NLProgram() {
}

NLProgram::~NLProgram() {
}

Column* NLProgram::allocColumn(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID: {
            auto column = std::make_unique<ColumnNodeIDs>();
            column->reserve(_chunkSize);
            return _columns.emplace_back(std::move(column)).get();
        }
        break;

        case NLChunkKind::EdgeID: {
            auto column = std::make_unique<ColumnEdgeIDs>();
            column->reserve(_chunkSize);
            return _columns.emplace_back(std::move(column)).get();
        }
        break;

        case NLChunkKind::EdgeTypeID: {
            auto column = std::make_unique<ColumnEdgeTypes>();
            column->reserve(_chunkSize);
            return _columns.emplace_back(std::move(column)).get();
        }
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}
