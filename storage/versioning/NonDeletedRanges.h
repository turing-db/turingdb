#pragma once

#include <stddef.h>
#include <vector>

namespace db {

struct NonDeletedRange {
    size_t start;
    size_t size;
};

using NonDeletedRanges = std::vector<NonDeletedRange>;
}
