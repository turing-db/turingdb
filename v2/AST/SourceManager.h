#pragma once

#include <cstdint>
#include <string_view>
#include <unordered_map>

#include "SourceLocation.h"

namespace db::v2 {

class SourceManager {
public:
    explicit SourceManager(std::string_view queryString);
    ~SourceManager();

    SourceManager(const SourceManager&) = delete;
    SourceManager(SourceManager&&) = delete;
    SourceManager& operator=(const SourceManager&) = delete;
    SourceManager& operator=(SourceManager&&) = delete;

    void setLocation(uintptr_t obj, const SourceLocation& loc);

    template <typename T>
    void setLocation(const T* obj, const SourceLocation& loc) {
        setLocation((uintptr_t)obj, loc);
    }

    const SourceLocation* getLocation(uintptr_t obj) const;
    std::string_view getStringRepr(uintptr_t obj) const;
    std::string_view getQueryString() const { return _queryString; }

private:
    std::string_view _queryString;
    std::unordered_map<uintptr_t, SourceLocation> _locations;
};

}
