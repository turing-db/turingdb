#pragma once

#include <span>

#include "PathExplorationDir.h"
#include "ID.h"

namespace db {

class PathHopFilter;

// One step of the body a walk repeats
struct PathStep {
    PathExplorationDir _direction {PathExplorationDir::FORWARD};
    std::span<const EdgeTypeID> _edgeTypes;
    PathHopFilter* _hopFilter {nullptr};
};

}
