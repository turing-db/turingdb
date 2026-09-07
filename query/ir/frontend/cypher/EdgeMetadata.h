#pragma once

#include <limits>
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

    static constexpr uint64_t UNBOUNDED_HOPS = std::numeric_limits<uint64_t>::max();

    explicit EdgeMetadata(EdgeType type)
        : _type(type)
    {
    }

    // A variable-length hop: the edge stands for every trail of minHops to maxHops edges
    EdgeMetadata(EdgeType type, uint64_t minHops, uint64_t maxHops)
        : _type(type),
        _quantified(true),
        _minHops(minHops),
        _maxHops(maxHops)
    {
    }

    EdgeType type() const { return _type; }

    bool isMetaEdge() const { return _type == EdgeType::MERGE; }

    bool isQuantified() const { return _quantified; }
    uint64_t getMinHops() const { return _minHops; }
    uint64_t getMaxHops() const { return _maxHops; }

    bool operator==(const EdgeMetadata& other) const {
        return _type == other._type
            && _quantified == other._quantified
            && _minHops == other._minHops
            && _maxHops == other._maxHops;
    }

private:
    EdgeType _type {EdgeType::_SIZE};
    bool _quantified {false};
    uint64_t _minHops {0};
    uint64_t _maxHops {0};
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
