#pragma once

#include <stdint.h>
#include <type_traits>

#include "EnumToString.h"

namespace db {

class EdgeMetadata {
public:
    enum class EdgeType : uint8_t {
        GET_OUT_EDGES,
        GET_IN_EDGES,
        GET_EDGES,
        GET_EDGE_TGT,
        GET_EDGE_SRC,
        MERGE,

        _SIZE
    };

    explicit EdgeMetadata(EdgeType type)
        : _type(type)
    {
    }

    EdgeType type() const { return _type; }

    bool isMetaEdge() const { return _type == EdgeType::MERGE; }

    bool operator==(const EdgeMetadata& other) const {
        return _type == other._type;
    }

private:
    EdgeType _type {EdgeType::_SIZE};
};

static_assert(std::is_trivially_copyable_v<EdgeMetadata>);

using EdgeTypeName = EnumToString<EdgeMetadata::EdgeType>::Create<
    EnumStringPair<EdgeMetadata::EdgeType::GET_OUT_EDGES, "getout">,
    EnumStringPair<EdgeMetadata::EdgeType::GET_IN_EDGES, "getin">,
    EnumStringPair<EdgeMetadata::EdgeType::GET_EDGES, "bidir">,
    EnumStringPair<EdgeMetadata::EdgeType::GET_EDGE_TGT, "edge_tgt">,
    EnumStringPair<EdgeMetadata::EdgeType::GET_EDGE_SRC, "edge_src">,
    EnumStringPair<EdgeMetadata::EdgeType::MERGE, "merge">
>;

}
