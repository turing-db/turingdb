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

size_t StringApproximatorIndex::charToIndex(char c) {
    // Children array layout:
    // INDEX CHARACTER VALUE 0     a
    // ...  ...
    // 25    z
    // 26    0
    // ...  ...
    // 36    9

    // NOTE: Converts upper-case characters to lower to calculate index,
    // but the value of the node is still uppercase
    if (isalpha(c)) return std::tolower(c, std::locale()) - 'a';
    else if (isdigit(c)) return 26 + c - '0';
    else throw std::runtime_error("Invalid character: " + std::to_string(c));
}

void StringApproximatorIndex::insert(std::string_view str, NodeID owner) {
    PrefixTreeNode* node = this->_root.get();
    for (const char c : str) {
        const size_t idx = charToIndex(c);
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

const StringApproximatorIndex::PrefixTreeIterator
StringApproximatorIndex::find(std::string_view sv) const {
    PrefixTreeNode* node = this->_root.get();
    for (const char c : sv) {
        const size_t idx = charToIndex(c);
        if (!node->_children[idx]) {
            return PrefixTreeIterator{NOT_FOUND, node};
        }
        node = node->_children[idx].get();
    }
    const FindResult res = node->_isComplete ? FOUND : FOUND_PREFIX;
    return PrefixTreeIterator{res, node};
}


void StringApproximatorIndex::getOwners(std::vector<NodeID>& owners, std::string_view str) {
    owners.clear(); // NOTE: Doesn't deallocate or change capacity
    const auto it = find(str);
    if (!it._nodePtr) return;

    PrefixTreeNode* node = it._nodePtr;
    std::deque<PrefixTreeNode*> q{node};
    // BFS, collecting owners
    while (!q.empty()) {
        const PrefixTreeNode* n = q.front();
        q.pop_front();
        for (size_t i{0}; i < n->_children.size(); i++) {
            if (PrefixTreeNode* child = n->_children[i].get()) {
                q.push_back(child);
            }
        }
        // Collect owners to report back
        if (n->_isComplete) {
            std::vector<NodeID>* nOwners = n->_owners.get();
            owners.insert(std::end(owners),
                       std::begin(*nOwners),
                       std::end(*nOwners));
        }
    }
}

void StringApproximatorIndex::print() const {
    printTree(this->_root.get(), "", false);
}

void StringApproximatorIndex::printTree(StringApproximatorIndex::PrefixTreeNode* node,
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
        printTree(kids[k], nextPrefix, k + 1 == kids.size());
    }
}

