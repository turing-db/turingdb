#pragma once

#include <unordered_set>

namespace db::v2 {

class VarDecl;

/// @brief A cache of entity type dependencies.
//
// @details Allows to retrieve cache/retrieve the expression that generates the entity type
//          in the plan graph.
class GetEntityTypeCache {
public:
    struct Dependency {
        const VarDecl* _entityDecl {nullptr};
        const VarDecl* _exprDecl {nullptr};

        struct Hash {
            std::size_t operator()(const Dependency& d) const {
                return std::hash<const VarDecl*> {}(d._entityDecl);
            }
        };

        struct Equal {
            bool operator()(const Dependency& a, const Dependency& b) const {
                return a._entityDecl == b._entityDecl;
            }
        };
    };

    /// @brief Adds a dependency into the cache. Returns nullptr if the dependency was
    ///        added. Otherwise, returns the cached dependency.
    //
    // @details Unique way to interact with the cache. If an attempt to insert
    //          a dependency returns a non-null pointer, it means that the dependency was
    //          already present in the cache.
    //
    // @return nullptr if the dependency was inserted, the cached dependency otherwise.
    const Dependency* cacheOrRetrieve(const VarDecl* entityDecl, const VarDecl* exprDecl) {
        auto pair = _dependencies.insert({entityDecl, exprDecl});
        if (!pair.second) {
            return nullptr;
        }

        return &*pair.first;
    }

private:
    std::unordered_set<Dependency, Dependency::Hash, Dependency::Equal> _dependencies;
};

}
