#include "SourceManager.h"

using namespace db::v2;

SourceManager::SourceManager(std::string_view queryString)
    : _queryString(queryString)
{
}

SourceManager::~SourceManager() {
}

void SourceManager::setLocation(uintptr_t obj, const SourceLocation& loc) {
    if (!_debugLocations) {
        return;
    }

    _locations[obj] = loc;
}

const SourceLocation* SourceManager::getLocation(uintptr_t obj) const {
    const auto it = _locations.find(obj);
    if (it == _locations.end()) {
        return nullptr;
    }

    return &it->second;
}
