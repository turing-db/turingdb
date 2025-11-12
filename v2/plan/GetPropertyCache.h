#pragma once

#include <string_view>
#include <unordered_set>

namespace db::v2 {

class VarDecl;

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

    bool insert(const VarDecl* var, std::string_view property) {
        return _dependencies.insert({var, property}).second;
    }

private:
    std::unordered_set<Dependency, Dependency::Hash, Dependency::Equal> _dependencies;
};

}
