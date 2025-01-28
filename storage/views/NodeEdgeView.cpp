#include "NodeEdgeView.h"

namespace db {

size_t NodeEdgeView::getOutEdgeCount() const {
    return _outCount;
}

size_t NodeEdgeView::getInEdgeCount() const {
    return _inCount;
}

void NodeEdgeView::addOuts(std::span<const EdgeRecord> outs) {
    _outEdgeSpans.push_back(outs);
    _outCount += outs.size();
}

void NodeEdgeView::addIns(std::span<const EdgeRecord> ins) {
    _inEdgeSpans.push_back(ins);
    _inCount += ins.size();
}

}
