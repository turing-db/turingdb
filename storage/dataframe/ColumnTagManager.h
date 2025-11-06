#pragma once

#include "ColumnTag.h"

namespace db {

class DataframeManager;

class ColumnTagManager {
public:
    friend DataframeManager;

    ColumnTag allocTag() {
        const ColumnTag tag = _nextFreeTag;
        _nextFreeTag++;
        return tag;
    }

private:
    ColumnTag _nextFreeTag {0};

    ColumnTagManager() = default;
    ~ColumnTagManager() = default;
};

}
