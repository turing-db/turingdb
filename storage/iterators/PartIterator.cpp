#include "PartIterator.h"

#include "datapart/DataPart.h"

using namespace db;

PartIterator::PartIterator(const GraphView& view)
    : _it(view.dataparts().begin()),
    _itEnd(view.dataparts().end()),
    _itStart(view.dataparts().begin())
{
}

void PartIterator::skipEmptyParts() {
    for (; isNotEnd(); next()) {
        const DataPart* part = get();
        if (part->getNodeContainerSize() != 0) {
            return;
        }
    }
}
