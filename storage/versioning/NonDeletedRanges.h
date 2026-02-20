#pragma once

#include <stddef.h>
#include <vector>

namespace db {

struct NonDeletedRange {
    size_t _start {0};
    size_t _size {0};
};

using NonDeletedRanges = std::vector<NonDeletedRange>;
}
