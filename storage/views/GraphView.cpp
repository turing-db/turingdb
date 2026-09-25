#include "GraphView.h"

#include <type_traits>

#include "reader/GraphReader.h"

using namespace db;

GraphReader GraphView::read() const {
    return GraphReader(*this);
}

void GraphView::setChangeDeletions(const DeletedNodes* nodes, const DeletedEdges* edges) {
    _changeDeletedNodes = nodes;
    _changeDeletedEdges = edges;
}

template <TypedInternalID IDT>
bool GraphView::isDeleted(IDT id) const {
    if (_data->tombstones().contains(id)) {
        return true;
    }

    if constexpr (std::is_same_v<IDT, NodeID>) {
        return _changeDeletedNodes && _changeDeletedNodes->contains(id);
    } else {
        return _changeDeletedEdges && _changeDeletedEdges->contains(id);
    }
}

bool GraphView::hasDeletedNodes() const {
    return _data->tombstones().hasNodes() || (_changeDeletedNodes && !_changeDeletedNodes->empty());
}

bool GraphView::hasDeletedEdges() const {
    return _data->tombstones().hasEdges() || (_changeDeletedEdges && !_changeDeletedEdges->empty());
}

namespace db {

template bool GraphView::isDeleted<NodeID>(NodeID id) const;
template bool GraphView::isDeleted<EdgeID>(EdgeID id) const;

}
