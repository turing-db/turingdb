#pragma once

#include <stdint.h>

namespace db {

enum class PathExplorationDir : uint8_t {
    FORWARD,
    BACKWARD,
    BOTH,
};

// The direction a search from the ends takes, which is the walk's reversed: the nodes a
// forward walk arrives at are the ones an in-edge leaves
constexpr PathExplorationDir reverseOf(PathExplorationDir direction) {
    switch (direction) {
        case PathExplorationDir::FORWARD:
            return PathExplorationDir::BACKWARD;
        break;
        case PathExplorationDir::BACKWARD:
            return PathExplorationDir::FORWARD;
        break;
        case PathExplorationDir::BOTH:
            return PathExplorationDir::BOTH;
        break;
    }

    return PathExplorationDir::BOTH;
}

}
