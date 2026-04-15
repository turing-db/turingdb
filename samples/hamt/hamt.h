#include "ArcManager.h"

namespace db {

class HAMTIndex {
class HAMTNode;
class HAMTLeaf;

public:
    using Bitmask = uint32_t;

private:
    WeakArc<HAMTNode> _root;
};

class HAMTIndex::HAMTNode {
public:
    Bitmask mask() const { return _mask; }

private:
    Bitmask _mask {0};
};

class HAMTIndex::HAMTLeaf {
};

}
