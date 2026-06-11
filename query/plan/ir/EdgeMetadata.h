#pragma once

#include <stdint.h>
#include <type_traits>

#include "EnumToString.h"

namespace db {

class EdgeMetadata {
public:
    enum class EdgeType : uint8_t;

    explicit EdgeMetadata(EdgeType type)
        : _type(type)
    {
    }

    enum class EdgeType : uint8_t {
        OUTGOING,
        INCOMING,
        BIDIRECTIONAL,
        MERGE,

        _SIZE
    };

    EdgeType type() const { return _type; }

    static bool isMetaEdge(EdgeType et) { return et == EdgeType::MERGE; }

    bool operator==(const EdgeMetadata& other) const {
        return _type == other._type;
    }

private:
    EdgeType _type {EdgeType::_SIZE};
};

static_assert(std::is_trivially_copyable_v<EdgeMetadata>);

using EdgeTypeName = EnumToString<EdgeMetadata::EdgeType>::Create<
    EnumStringPair<EdgeMetadata::EdgeType::OUTGOING, "getout">,
    EnumStringPair<EdgeMetadata::EdgeType::INCOMING, "getin">,
    EnumStringPair<EdgeMetadata::EdgeType::BIDIRECTIONAL, "bidir">,
    EnumStringPair<EdgeMetadata::EdgeType::MERGE, "merge">
>;

}
