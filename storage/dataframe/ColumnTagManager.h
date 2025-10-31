#pragma once

#include "ColumnTag.h"

namespace db {

class ColumnTagManager {
public:
    ColumnTagManager() = default;
    ~ColumnTagManager() = default;

    ColumnTag allocTag() {
        const ColumnTag tag = _nextFreeTag;
        _nextFreeTag++;
        return tag;
    }

private:
    ColumnTag _nextFreeTag {0};
};

}
