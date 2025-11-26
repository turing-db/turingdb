#pragma once

#include <string_view>
#include <unordered_set>

namespace db::v2 {

class VarDecl;

/// @brief A cache of property dependencies.
//
// @details Its sole purpose is to know if a property dependency has already been
//          handled, in which case, it is already present in the cache
class GetPropertyCache {
public:
    struct Dependency {
        const VarDecl* _entityDecl {nullptr};
        const VarDecl* _exprDecl {nullptr};
        std::string_view _property;

        struct Hash {
            std::size_t operator()(const Dependency& d) const {
                return std::hash<const VarDecl*> {}(d._entityDecl)
                     ^ std::hash<std::string_view> {}(d._property);
            }
        };

        struct Equal {
            bool operator()(const Dependency& a, const Dependency& b) const {
                return a._entityDecl == b._entityDecl && a._property == b._property;
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
    const Dependency* cacheOrRetrieve(const VarDecl* entityDecl, const VarDecl* exprDecl, std::string_view property) {
        auto pair = _dependencies.insert({entityDecl, exprDecl, property});
        if (pair.second) {
            return nullptr;
        }

        return &*pair.first;
    }

private:
    std::unordered_set<Dependency, Dependency::Hash, Dependency::Equal> _dependencies;
};

}
