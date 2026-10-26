#include "SourceManager.h"

using namespace db;

SourceManager::SourceManager(std::string_view queryString)
    : _queryString(queryString)
{
}

SourceManager::~SourceManager() {
}

void SourceManager::setLocation(uintptr_t obj, const SourceLocation& loc) {
    _locations[obj] = loc;
}

std::string_view SourceManager::getStringRepr(uintptr_t obj) const {
    const SourceLocation* loc = getLocation(obj);
    if (!loc) {
        return "";
    }

    return _queryString.substr(loc->_beginOffset, loc->_endOffset - loc->_beginOffset);
}

const SourceLocation* SourceManager::getLocation(uintptr_t obj) const {
    const auto it = _locations.find(obj);
    if (it == _locations.end()) {
        return nullptr;
    }

    return &it->second;
}
