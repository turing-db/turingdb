#include "NLProgram.h"

#include "BioAssert.h"

using namespace db;

NLFunctionData::~NLFunctionData() {
}

NLProgram::NLProgram() {
}

NLProgram::~NLProgram() {
}

Column* NLProgram::addChunkSlot(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID: {
            auto column = std::make_unique<ColumnNodeIDs>();
            column->reserve(_chunkSize);
            return _chunkSlots.emplace_back(std::move(column)).get();
        }
        break;

        case NLChunkKind::EdgeID: {
            auto column = std::make_unique<ColumnEdgeIDs>();
            column->reserve(_chunkSize);
            return _chunkSlots.emplace_back(std::move(column)).get();
        }
        break;

        case NLChunkKind::EdgeTypeID: {
            auto column = std::make_unique<ColumnEdgeTypes>();
            column->reserve(_chunkSize);
            return _chunkSlots.emplace_back(std::move(column)).get();
        }
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}
