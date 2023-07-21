#include "ExploratorTree.h"

using namespace db;

// ExploratorTreeNode

ExploratorTreeNode::ExploratorTreeNode(Node* current)
    : _current(current)
{
}

ExploratorTreeNode::~ExploratorTreeNode() {
}

ExploratorTreeNode* ExploratorTreeNode::create(ExploratorTree* tree,
                                               db::Node* current,
                                               ExploratorTreeNode* parent) {
    ExploratorTreeNode* node = new ExploratorTreeNode(current);
    tree->registerNode(node);

    if (parent) {
        node->setParent(parent);
        parent->addChild(node);
    } else {
        tree->addRoot(node);
    }

    return node;
}

void ExploratorTreeNode::addChild(ExploratorTreeNode* child) {
    _children.push_back(child);
}

// ExploratorTree

ExploratorTree::ExploratorTree()
{
}

ExploratorTree::~ExploratorTree() {
}


void ExploratorTree::registerNode(ExploratorTreeNode* node) {
    _nodes[node->getNode()] = node;
}

void ExploratorTree::addRoot(ExploratorTreeNode* root) {
    _roots.push_back(root);
}

ExploratorTreeNode* ExploratorTree::getNode(db::Node* node) const {
    const auto it = _nodes.find(node);
    if (it == _nodes.end()) {
        return nullptr;
    }

    return it->second;
}
