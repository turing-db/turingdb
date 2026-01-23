#include "Tombstones.h"

#include <type_traits>

#include "ID.h"

using namespace db;

namespace db {

template bool Tombstones::contains<NodeID>(NodeID id) const;
template bool Tombstones::contains<EdgeID>(EdgeID id) const;

}

template <TypedInternalID IDT>
bool Tombstones::contains(IDT id) const {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        return containsNode(id);
    }
    else {
        return containsEdge(id);
    }
}
