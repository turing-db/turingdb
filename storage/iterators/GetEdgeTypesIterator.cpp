#include "GetEdgeTypesIterator.h"

#include <algorithm>

#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"

#include "BioAssert.h"

using namespace db;

GetEdgeTypesIterator::GetEdgeTypesIterator(const GraphView& view,
                                           const ColumnEdgeIDs* inputEdgeIDs)
    : Iterator(view),
    _inputEdgeIDs(inputEdgeIDs),
    _edgeIt(inputEdgeIDs->cbegin())
{
}

GetEdgeTypesIterator::~GetEdgeTypesIterator() {
}

void GetEdgeTypesIterator::next() {
    ++_edgeIt;
}

EdgeTypeID GetEdgeTypesIterator::get() const {
    const EdgeID edgeID = *_edgeIt;

    for (const auto& part : _view.dataparts()) {
        const auto* edge = part->edges().tryGet(edgeID);
        if (edge) {
            return edge->_edgeTypeID;
        }
    }

    return EdgeTypeID {};
}

GetEdgeTypesChunkWriter::GetEdgeTypesChunkWriter(const GraphView& view,
                                                 const ColumnEdgeIDs* inputEdgeIDs)
    : GetEdgeTypesIterator(view, inputEdgeIDs)
{
}

void GetEdgeTypesChunkWriter::fill(size_t maxCount) {
    bioassert(_edgeTypes, "GetEdgeTypesChunkWriter must be initialized with a valid column");

    const size_t remainingInput = static_cast<size_t>(
        std::distance(_edgeIt, _inputEdgeIDs->cend()));
    const size_t count = std::min(maxCount, remainingInput);

    _edgeTypes->resize(count);
    std::generate(_edgeTypes->begin(), _edgeTypes->end(), [this]() {
        const EdgeTypeID id = get();
        next();
        return id;
    });
}
