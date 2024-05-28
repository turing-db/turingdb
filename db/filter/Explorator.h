#pragma once

#include <vector>
#include <memory>
#include <set>
#include <unordered_set>
#include <string>

#include "Writeback.h"
#include "StringRef.h"
#include "DBObject.h"
#include "ExploratorTree.h"

#include "SubNetBuilder.h"

namespace db {
class DB;
class Node;
}

// Explore a database from a set of seed nodes
// up to some maximum distance
class Explorator {
public:
    using TreeNodes = std::set<ExploratorTreeNode*, ExploratorTreeNode::Comparator>;

    enum class TargetNameMatchType {
        EXACT, PREFIX, SUBWORD
    };

    Explorator(db::DB* db);
    ~Explorator();

    void addSeeds(const std::vector<db::Node*>& seeds);

    void setMaximumDistance(size_t maxDist) { _maxDist = maxDist; }
    void setMaximumDegree(size_t degree) { _maxDegree = degree; }
    void setTraverseTargets(bool enabled) { _traverseTargets = enabled; }
    void setTraversePathways(bool enabled) { _traversePathways = enabled; }
    void setTraverseSets(bool enabled) { _traverseSets = enabled; }
    void setTraverseFailedReaction(bool enabled) { _traverseFailedReaction = enabled; }

    void setNoDefaultTargets();
    void addTargetClass(const std::string& name);
    void addTargetNodeName(const std::string& name);
    void setTargetNameMatchType(TargetNameMatchType matchType) { _targetNameMatchType = matchType; }

    void addExcludedName(const std::string& name);
    void addDefaultExcludedNames();

    void addExcludedClass(const std::string& schemaClass);
    void addDefaultExcludedClasses();

    void run();

    db::Network* getResultNet() const { return _resNet; }

    const TreeNodes& targets() const { return _targets; }

private:
    size_t _maxDist {5};
    size_t _maxDegree {0};
    bool _traverseTargets {true};
    bool _traversePathways {true};
    bool _traverseSets {true};
    bool _traverseFailedReaction {false};

    db::DB* _db {nullptr};
    db::Writeback _wb;
    std::vector<db::Node*> _seeds;
    ExploratorTree* _tree {nullptr};
    db::Network* _resNet {nullptr};
    std::unique_ptr<SubNetBuilder> _subNetBuilder;

    std::unordered_set<std::string> _excludedNames;
    std::unordered_set<std::string> _excludedClasses;
    std::unordered_set<std::string> _targetClasses;
    std::unordered_set<std::string> _targetNodeNames;

    TargetNameMatchType _targetNameMatchType {TargetNameMatchType::PREFIX};

    TreeNodes _targets;

    db::StringRef _displayName;
    db::StringRef _speciesName;
    db::StringRef _schemaClassName;

    bool shouldExplore(const db::Node* node, const db::Edge* edge) const;
    void buildNetwork();
    bool hasTargetSchemaClass(const db::Node* node) const;
    bool hasTargetName(const db::Node* node) const;
    void addDefaultTargetClasses();
    bool isTargetNameMatch(const std::string& target, const std::string& displayName) const;
    bool isTarget(const db::Node* node) const;
};
