#pragma once

#include <stdint.h>

namespace db {

class LabelID {
public:
    LabelID() = default;

    LabelID(uint64_t id)
        : _id(id)
    {
    }

    uint64_t getID() const { return _id; }

private:
    uint64_t _id {0};
};

}