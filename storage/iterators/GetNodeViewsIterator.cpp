#include "GetNodeViewsIterator.h"

#include <algorithm>
#include <numeric>

#include "reader/GraphReader.h"
#include "IteratorUtils.h"

namespace db {

GetNodeViewsIterator::GetNodeViewsIterator(const GraphView& view,
                                           const ColumnNodeIDs* inputNodeIDs)
    : Iterator(view),
      _inputNodeIDs(inputNodeIDs),
      _nodeIt(inputNodeIDs->cbegin()),
      _nodeItEnd(inputNodeIDs->cend())
{
    const auto reader = view.read();
    if (_nodeIt != _nodeItEnd) {
        _nodeView = reader.getNodeView(*_nodeIt);
    }
}

GetNodeViewsIterator::~GetNodeViewsIterator() = default;

void GetNodeViewsIterator::next() {
    _nodeIt++;

    if (_nodeIt == _nodeItEnd) {
        _partIt.skipToEnd();
        return;
    }

    _nodeView = _view.read().getNodeView(*_nodeIt);
}

GetNodeViewsChunkWriter::GetNodeViewsChunkWriter(const GraphView& view,
                                                 const ColumnNodeIDs* inputNodeIDs)
    : GetNodeViewsIterator(view, inputNodeIDs)
{
}

static constexpr size_t NColumns = 1;
static constexpr size_t NCombinations = 1 << NColumns;

void GetNodeViewsChunkWriter::fill(size_t chunkSize) {
    size_t remainingSpace = chunkSize;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    _indices->clear();

    if (_nodeViews) {
        _nodeViews->clear();
    }
    const auto reader = _view.read();

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (isValid() && remainingSpace > 0) {
            const size_t remainingNodes = std::distance(_nodeIt, _nodeItEnd);
            const size_t rangeSize = std::min(remainingSpace, remainingNodes);
            const size_t prevSize = _indices->size();
            const size_t newSize = prevSize + rangeSize;
            _indices->resize(newSize);

            const size_t index = std::distance(_inputNodeIDs->cbegin(), _nodeIt);
            std::iota(_indices->begin() + prevSize, _indices->end(), index);
            remainingSpace -= rangeSize;

            if constexpr (conditions[0]) {
                _nodeViews->resize(newSize);
                std::generate((_nodeViews)->begin() + prevSize,
                              (_nodeViews)->end(),
                              [nodeIt = this->_nodeIt, &reader]() mutable {
                                  return reader.getNodeView(*(nodeIt++));
                              });
            }

            _nodeIt += rangeSize;
            if (_nodeIt == _nodeItEnd) {
                _partIt.skipToEnd();
                return;
            }

            _nodeView = reader.getNodeView(*_nodeIt);
        };
    };

    switch (bitmask::create(_nodeViews)) {
        CASE(0);
        CASE(1);
    }
}

}

