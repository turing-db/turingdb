#pragma once

#include <stdint.h>

namespace db {

class EntityID {
public:
    EntityID(uint64_t id)
        : _id(id)
    {
    }

    uint64_t getID() const { return _id; }

private:
    uint64_t _id{0};
};

}