#include "indexes/StringIndex.h"

namespace db {

class StringIndexDumpConstants {
public:
    static constexpr size_t MAXNODESIZE =
        sizeof(size_t) + // NodeID
        sizeof(size_t) + // Number of non-null children
        // Worse case: all children, for each write index in child array and ID in
        // @ref _nodeManager
        (StringIndex::PrefixTreeNode::ALPHABET_SIZE) * (sizeof(size_t) + sizeof(size_t))
        + sizeof(size_t); // Size of owners array
};
}
