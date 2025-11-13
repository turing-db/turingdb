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
        const VarDecl* _var {nullptr};
        std::string_view _property;

        struct Hash {
            std::size_t operator()(const Dependency& d) const {
                return std::hash<const VarDecl*> {}(d._var)
                     ^ std::hash<std::string_view> {}(d._property);
            }
        };

        struct Equal {
            bool operator()(const Dependency& a, const Dependency& b) const {
                return a._var == b._var && a._property == b._property;
            }
        };
    };

    /// @brief Inserts a dependency into the cache.
    //
    // @details Unique way to interact with the cache. If an attempt to insert
    //          a dependency fails, it means that the dependency was already
    //          handled.
    //
    // @return true if the dependency was inserted, false if it was already present.
    bool insert(const VarDecl* var, std::string_view property) {
        return _dependencies.insert({var, property}).second;
    }

private:
    std::unordered_set<Dependency, Dependency::Hash, Dependency::Equal> _dependencies;
};

}
