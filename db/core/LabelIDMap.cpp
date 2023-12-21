#include "LabelIDMap.h"

using namespace db;

LabelIDMap::LabelIDMap()
{
}

LabelIDMap::~LabelIDMap() {
}

LabelID LabelIDMap::getLabelID(const std::string& str) {
    LabelID labelID;
    _lock.lock();

    const auto it = _map.find(str);
    if (it == _map.end()) {
        const uint64_t nextID = _nextFreeID;
        _nextFreeID++;
        _map[str] = nextID;
        labelID = LabelID(nextID);
    } else {
        labelID = LabelID(it->second);
    }

    _lock.unlock();
    return labelID;
}