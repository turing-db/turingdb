#include "SourceManager.h"
#include "spdlog/fmt/bundled/base.h"

using namespace db::v2;

namespace {

std::string_view nextLine(std::string_view& str) {
    const auto pos = str.find('\n');
    if (pos == std::string_view::npos) {
        auto line = str;
        str = str.substr(str.size(), str.size());

        return line;
    }

    auto line = str.substr(0, pos);
    str = str.substr(pos + 1, str.size());
    return line;
}

void ignoreLines(std::string_view& str, size_t count) {
    for (size_t i = 0; i < count; i++) {
        nextLine(str);
    }
}

}

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

    std::string_view repr = _queryString;

    const size_t firstLineNo = loc->beginLine;
    const size_t lastLineNo = loc->endLine;
    size_t lastCol = loc->endColumn;

    ignoreLines(repr, firstLineNo - 1);
    repr = repr.substr(loc->beginColumn - 1);

    if (firstLineNo != lastLineNo) {
        ignoreLines(repr, lastLineNo - firstLineNo);
        nextLine(repr);
    } else {
        lastCol = loc->endColumn - loc->beginColumn;
    }

    repr = repr.substr(0, lastCol);
    return repr;
}

const SourceLocation* SourceManager::getLocation(uintptr_t obj) const {
    const auto it = _locations.find(obj);
    if (it == _locations.end()) {
        return nullptr;
    }

    return &it->second;
}
