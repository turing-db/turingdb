#pragma once

#include <string>
#include <unordered_map>
#include <stdint.h>

#include "LabelID.h"
#include "SpinLock.h"

namespace db {

class LabelIDMap {
public:
    LabelIDMap();
    ~LabelIDMap();

    LabelID getLabelID(const std::string& str);

private:
    SpinLock _lock;
    uint64_t _nextFreeID {0};
    std::unordered_map<std::string, uint64_t> _map;
};

}