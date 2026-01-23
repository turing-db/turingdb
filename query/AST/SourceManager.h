#pragma once

#include <stdint.h>
#include <string_view>
#include <unordered_map>

#include "SourceLocation.h"

namespace db {

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
    std::string_view getQueryString() const { return _queryString; }

    /** @brief Get the substring of the query that generates the passed AST object
     *
     * @details This function returns the substring of the query that maps to AST object
     * e.g., if the query is `MATCH (a) RETURN a.age > 20`, and we pass the `Expr*` that
     * represents `a.age > 20` to this function, it returns the string `"a.age > 20"`
     *
     * @param obj The AST object
     *
     * @return The substring of the query that maps to the given AST object
     */
    std::string_view getStringRepr(uintptr_t obj) const;

private:
    std::string_view _queryString;
    std::unordered_map<uintptr_t, SourceLocation> _locations;
};

}
