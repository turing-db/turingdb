#pragma once

#include <vector>
#include <memory>

#include "Writeback.h"
#include "StringRef.h"
#include "SubNetBuilder.h"

namespace db {
class DB;
class Node;
}

class ExploratorTree;
class ExploratorTreeNode;

// Explore a database from a set of seed nodes
// up to some maximum distance
class Explorator {
public:
    Explorator(db::DB* db);
    ~Explorator();

    void addSeeds(const std::vector<db::Node*>& seeds);

    void setMaximumDistance(size_t maxDist) { _maxDist = maxDist; }

    void run();

    db::Network* getResultNet() const { return _resNet; }

private:
    size_t _maxDist {10};
    db::DB* _db {nullptr};
    db::Writeback _wb;
    std::vector<db::Node*> _seeds;
    ExploratorTree* _tree {nullptr};
    db::Network* _resNet {nullptr};
    std::unique_ptr<SubNetBuilder> _subNetBuilder;

    db::StringRef _stIdName;
    db::StringRef _displayName;
    db::StringRef _speciesName;

    void visit(ExploratorTreeNode* node);
    bool shouldExplore(const db::Node* node);
};
