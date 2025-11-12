#pragma once

#include <unordered_set>

namespace db::v2 {

class VarDecl;

class GetEntityTypeCache {
public:
    bool insert(const VarDecl* var) {
        return _dependencies.insert(var).second;
    }

private:
    std::unordered_set<const VarDecl*> _dependencies;
};

}
