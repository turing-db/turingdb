#pragma once

#include <unordered_set>

namespace db::v2 {

class VarDecl;

/// @brief A cache of entity type dependencies.
//
// @details Its sole purpose is to know if an entity type  dependency has already been
//          handled, in which case, it is already present in the cache
class GetEntityTypeCache {
public:
    /// @brief Inserts a dependency into the cache.
    //
    // @details Unique way to interact with the cache. If an attempt to insert
    //          a dependency fails, it means that the dependency was already
    //          handled.
    //
    // @return true if the dependency was inserted, false if it was already present.
    bool insert(const VarDecl* var) {
        return _dependencies.insert(var).second;
    }

private:
    std::unordered_set<const VarDecl*> _dependencies;
};

}
