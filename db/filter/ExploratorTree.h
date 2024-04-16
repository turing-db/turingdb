#pragma once

#include <vector>
#include <map>

#include "Node.h"

class ExploratorTree;

class ExploratorTreeNode {
public:
    friend ExploratorTree;
    using Nodes = std::vector<ExploratorTreeNode*>;

    struct Comparator {
        bool operator()(const ExploratorTreeNode* lhs,
                        const ExploratorTreeNode* rhs) const {
            return db::DBObject::Sorter()(lhs->getNode(), rhs->getNode());
        }
    };

    static ExploratorTreeNode* create(ExploratorTree* tree,
                                      db::Node* current,
                                      ExploratorTreeNode* parent);

    db::Node* getNode() const { return _current; }

    ExploratorTreeNode* getParent() const { return _parent; }

    void setDistance(size_t dist) { _dist = dist; }
    size_t getDistance() const { return _dist; }

    const Nodes& children() const { return _children; }

private:
    db::Node* _current {nullptr};
    ExploratorTreeNode* _parent {nullptr};
    size_t _dist {0};
    Nodes _children;

    explicit ExploratorTreeNode(db::Node* current);
    ~ExploratorTreeNode();
    void addChild(ExploratorTreeNode* child);
    void setParent(ExploratorTreeNode* parent) { _parent = parent; }
};

class ExploratorTree {
public:
    friend ExploratorTreeNode;
    using Nodes = std::vector<ExploratorTreeNode*>;

    explicit ExploratorTree();
    ~ExploratorTree();

    const Nodes& roots() const { return _roots; }

    ExploratorTreeNode* getNode(db::Node* node) const;

    size_t getSize() const { return _nodes.size(); }

private:
    std::vector<ExploratorTreeNode*> _roots;
    std::map<db::Node*, ExploratorTreeNode*, db::DBObject::Sorter> _nodes;

    void registerNode(ExploratorTreeNode* node);
    void addRoot(ExploratorTreeNode* root);
};
