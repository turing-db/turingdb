#pragma once

#include <variant>
#include <unordered_set>

#include "metadata/LabelSet.h"

namespace db::v2 {

class VarNode;

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
        VarNode* _var {nullptr};
        Dependency _dep;

        struct Hasher {
            size_t operator()(const ExprDependency& v) const {
                return std::hash<const VarNode*> {}(v._var);
            }
        };

        struct Predicate {
            bool operator()(const ExprDependency& a, const ExprDependency& b) const {
                return a._var== b._var;
            }
        };
    };

    using Container = std::unordered_set<ExprDependency, ExprDependency::Hasher, ExprDependency::Predicate>;

    const Container& getDependencies() const {
        return _dependencies;
    }

    void addDependency(VarNode* var, const LabelSet& labelset) {
        _dependencies.emplace(var, LabelDependency {&labelset});
    }

    void addDependency(VarNode* var, PropertyTypeID propTypeID) {
        _dependencies.emplace(var, PropertyDependency {propTypeID});
    }

private:
    Container _dependencies;
};

}
