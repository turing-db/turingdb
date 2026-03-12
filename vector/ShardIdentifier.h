#pragma once

#include "LSHSignature.h"
#include "VecLibMetadata.h"

namespace vec {

struct ShardIdentifier {
    VecLibID _libID {0};
    LSHSignature _signature {0};

    struct Hash {
        std::size_t operator()(const ShardIdentifier& id) const {
            constexpr std::hash<VecLibID> h;
            return h(id._libID) ^ h(id._signature);
        }
    };

    struct Equal {
        bool operator()(const ShardIdentifier& lhs, const ShardIdentifier& rhs) const {
            return lhs._libID == rhs._libID
                && lhs._signature == rhs._signature;
        }
    };
};

}
