#include "prefix-tree.h"

#include <cctype>
#include <deque>
#include <memory>
#include <stdexcept>
#include <string>
#include <iostream>



StringApproximatorIndex::StringApproximatorIndex()
    : _root(std::make_unique<PrefixTreeNode>('\1'))
{
}
    
void StringApproximatorIndex::insert(std::string_view str) { return _insert(_root.get(), str, _currentID); }

StringApproximatorIndex::PrefixTreeIterator StringApproximatorIndex::find(std::string_view str) { return _find(_root.get(), str); }

std::vector<NodeID> StringApproximatorIndex::getOwners(std::string_view str) {
    auto it = find(str);
    if (!it._nodePtr) return {};
    return getOwners(it);
}

size_t StringApproximatorIndex::charToIndex(char c) {
    // Children array layout:
    // INDEX CHARACTER VALUE
    // 0     a
    // ...  ...
    // 25    z
    // 26    0
    // ...  ...
    // 36    9
    if (isalpha(c)) return c - 'a';
    else if (isdigit(c)) return 26 + c - '0';
    else throw std::runtime_error("Invalid character: " + std::to_string(c));
}

void StringApproximatorIndex::_insert(PrefixTreeNode* root, std::string_view sv, NodeID owner) {
    PrefixTreeNode* node = root;
    for (const char c : sv) {
        size_t idx = charToIndex(c);
        if (!node->_children[idx]) {
            node->_children[idx] = std::make_unique<PrefixTreeNode>(c);
        }
        node = node->_children[idx].get();
    }
    node->_isComplete |= true;
    if (!node->_owners) {
        node->_owners = std::make_unique<std::vector<NodeID>>();
    }
    node->_owners->push_back(owner);
    _currentID++;
}


StringApproximatorIndex::PrefixTreeIterator StringApproximatorIndex::_find(PrefixTreeNode* root, std::string_view sv) {
    char firstChar = sv[0];
    size_t idx = charToIndex(firstChar);
    if (idx >= SIGMA) return PrefixTreeIterator{NOT_FOUND, nullptr};
    if (!root->_children[idx]) {
        return PrefixTreeIterator{NOT_FOUND, nullptr};
    }

    sv.remove_prefix(1);        

    PrefixTreeNode* node = root->_children[idx].get();
    for (const char c : sv) {
        size_t idx = charToIndex(c);
        if (!node->_children[idx]) {
            return PrefixTreeIterator{NOT_FOUND, node};
        }
        node = node->_children[idx].get();
    }
    FindResult res = node->_isComplete ? FOUND : FOUND_PREFIX;
    return PrefixTreeIterator{res, node};
}


std::vector<NodeID> StringApproximatorIndex::getOwners(PrefixTreeIterator it) {
    PrefixTreeNode* node = it._nodePtr;
    std::vector<NodeID> res{};
    std::deque<PrefixTreeNode*> q{node};
    // BFS, collecting owners
    while (!q.empty()) {
        PrefixTreeNode* n = q.front();
        q.pop_front();
        for (size_t i{0}; i < n->_children.size(); i++) {
            if (auto child = n->_children[i].get())
            q.push_back(child);
        }
        if (n->_isComplete) {
            std::vector<NodeID>* owners = n->_owners.get();
            res.insert(std::end(res),
                       std::begin(*owners),
                       std::end(*owners));
        }
    }
    return res;
}

void StringApproximatorIndex::_printTree(StringApproximatorIndex::PrefixTreeNode* node) const {
    _printTree(node, "", true);
}


void StringApproximatorIndex::_printTree(StringApproximatorIndex::PrefixTreeNode* node,
                const std::string& prefix,
                bool isLastChild) const {
    if (!node) return;

    if (node->_val != '\1') {
        std::cout << prefix
                  << (isLastChild ? "└── " : "├── ")
                  << node->_val
                  << (node->_isComplete ? "*" : "")
                  << '\n';
    }

    // Gather existing children so we know which one is the last
    std::vector<PrefixTreeNode*> kids;
    kids.reserve(SIGMA);
    for (std::size_t i = 0; i < SIGMA; ++i)
        if (node->_children[i]) kids.push_back(node->_children[i].get());

    // Prefix extension: keep vertical bar if this isn’t last
    std::string nextPrefix = prefix;
    if (node->_val != '\1')          // don’t add for sentinel root
        nextPrefix += (isLastChild ? "    " : "│   ");

    // Recurse over children
    for (std::size_t k = 0; k < kids.size(); ++k) {
        _printTree(kids[k], nextPrefix, k + 1 == kids.size());
    }
}

