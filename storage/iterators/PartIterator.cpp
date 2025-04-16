#include "PartIterator.h"

#include "DataPart.h"

using namespace db;

PartIterator::PartIterator(const GraphView& view)
    : _it(view.dataparts().begin()),
    _itEnd(view.dataparts().end())
{
}

void PartIterator::skipEmptyParts() {
    for (; isValid(); next()) {
        const DataPart* part = get();
        if (part->getNodeCount() != 0) {
            return;
        }
    }
}
