#pragma once

#include "ListView.h"

namespace db {

// A path held in a list: the NodeID and EdgeID elements it runs through, alternating, under
// a tag of its own so it reads back as a path rather than as that list
class PathView {
public:
    PathView();
    explicit PathView(ListView entities);

    ListView getEntities() const { return _entities; }

private:
    ListView _entities;
};

}
