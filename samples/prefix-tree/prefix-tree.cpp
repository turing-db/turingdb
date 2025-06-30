#include <cctype>
#include <cstdint>
#include <iostream>
#include <array>
#include <memory>
#include <string_view>
#include <deque>
#include <vector>

#include "utils.h"

using NodeID = uint32_t;

NodeID CURRENT_ID = 1;

// Size of our alphabet: assumes some preprocessing,
// so only a-z and 1-9
constexpr size_t SIGMA = 26 + 10;

//https://www.notion.so/turingbio/Approximate-String-Matching-21e3aad664c880dba168d3f65a3dac73
 
class StringApproximatorIndex {
public:
    enum FindResult {
        FOUND,
        FOUND_PREFIX,
        NOT_FOUND
    };

    struct PrefixTreeNode {
        using NodeOwners = std::vector<NodeID>;

        std::array<std::unique_ptr<PrefixTreeNode>, SIGMA> _children{};
        char _val{'\0'};
        bool _isComplete{false};
        std::unique_ptr<NodeOwners> _owners;

        PrefixTreeNode(char val)
            : _children{},
            _val{val},
            _isComplete{false},
            _owners{}

        {
        }
    };

    struct PrefixTreeIterator {
        FindResult _result{NOT_FOUND};
        PrefixTreeNode* _nodePtr{nullptr};
    };

    StringApproximatorIndex()
    {
        _root = std::make_unique<PrefixTreeNode>('\1');
    }

    void insert(std::string_view str) { return _insert(_root.get(), str); }

    PrefixTreeIterator find(std::string_view str) { return _find(_root.get(), str); }

    std::vector<NodeID> getOwners(std::string_view str) {
        auto it = find(str);
        if (!it._nodePtr) return {};
        return getOwners(it);
    }

    void print() const { _printTree(_root.get()); }


private:
    std::unique_ptr<PrefixTreeNode> _root{};

    static size_t charToIndex(char c) {
        // Children array layout:
        // INDEX CHARACTER VALUE
        // 0     a
        // ...  ...
        // 25    z
        // 26    0
        // ...  ...
        // 36    9
        if (isalpha(c)) return c - 'a';
        else return 26 + c - '0';
    }

    void _insert(PrefixTreeNode* root, std::string_view sv, NodeID owner=CURRENT_ID++) {
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
    }


    PrefixTreeIterator _find(PrefixTreeNode* root, std::string_view sv) {
        char firstChar = sv[0];
        size_t idx = charToIndex(firstChar);
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

    static std::vector<NodeID> getOwners(PrefixTreeIterator it) {
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

    void _printTree(PrefixTreeNode* node) const {
        _printTree(node, "", true);
    }

    void _printTree(PrefixTreeNode* node,
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
};


const std::string help = "usage: \n \
    insert a string into the trie: 'i <string>' \n \
    query a string in the trie: 'f <string>' \n \
    print the trie: 'p' \n \
    ";

int main() {
    std::cout << help << std::endl;

    StringApproximatorIndex tree{};

    std::string in;
    while (true) {
        std::cout << ">";
        getline(std::cin, in);
        auto tokens = split(in, " ");
        if (tokens[0] == "i") {
            if (tokens.size() != 2) std::cout << "i <param>" << std::endl;
            else tree.insert(tokens[1]);
        }
        else if (tokens[0] == "p") tree.print();
        else if (tokens[0] == "f") {
            if (tokens.size() != 2) std::cout << "f <param>" << std::endl;
            else {
                StringApproximatorIndex::PrefixTreeIterator res = tree.find(tokens[1]);
                std::string verb;
                if (res._result == StringApproximatorIndex::FOUND) {
                    verb = " found ";
                }
                else if (res._result == StringApproximatorIndex::FOUND_PREFIX) {
                    verb = " found as a subtring ";
                }
                else {
                    verb = " not found ";
                }
                std::cout << "String " << tokens[1] << verb << std::endl;
            }
            
        }
        else if (tokens[0] == "o") {
            if (tokens.size() != 2) std::cout << "o <param>" << std::endl;
            else {
                auto owners = tree.getOwners(tokens[1]);
                for (const auto o : owners) std::cout << o << "    ";
                std::cout << std::endl;
            }
        }
        else std::cout << "unkown cmd" << std::endl;
    }

}

