#pragma once

#include <variant>
#include <unordered_set>

#include "metadata/LabelSet.h"

namespace db::v2 {

class VarDecl;

class PredicateDependencies {
public:
    struct LabelDependency {
        const LabelSet* _labelSet {nullptr};
    };

    struct PropertyDependency {
        const PropertyTypeID _propTypeID;
    };

    using Dependency = std::variant<LabelDependency, PropertyDependency>;

    struct ExprDependency {
        const VarDecl* _decl {nullptr};
        Dependency _dep;

        struct Hasher {
            size_t operator()(const ExprDependency& v) const {
                return std::hash<const VarDecl*> {}(v._decl);
            }
        };

        struct Predicate {
            bool operator()(const ExprDependency& a, const ExprDependency& b) const {
                return a._decl == b._decl;
            }
        };
    };

    using Container = std::unordered_set<ExprDependency, ExprDependency::Hasher, ExprDependency::Predicate>;

    const Container& getDependencies() const {
        return _dependencies;
    }

    void addDependency(const VarDecl* decl, const LabelSet& labelset) {
        _dependencies.emplace(decl, LabelDependency {&labelset});
    }

    void addDependency(const VarDecl* decl, PropertyTypeID propTypeID) {
        _dependencies.emplace(decl, PropertyDependency {propTypeID});
    }

private:
    Container _dependencies;
};

}
