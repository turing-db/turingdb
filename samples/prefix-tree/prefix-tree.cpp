#include <cctype>
#include <iostream>
#include <array>
#include <memory>
#include <string_view>
#include <vector>

#include "utils.h"


// Size of our alphabet: assumes some preprocessing,
// so only a-z and 1-9
constexpr size_t SIGMA = 26 + 10;

class StringApproximatorIndex {
public:
    enum FindResult {
        FOUND,
        FOUND_PREFIX,
        NOT_FOUND
    };



    struct PrefixTreeNode {
        std::array<std::unique_ptr<PrefixTreeNode>, SIGMA> _children{};
        // Termination char denotes terminal node
        char _val{'\0'};
        bool _isComplete{false};

        PrefixTreeNode(char val)
            : _children{},
            _val{val},
            _isComplete(false)
        {
        }

        // ~PrefixTreeNode() {
        //     for.get() (auto child : _children) {
        //         delete child;
        //     }
        // }

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

    void _insert(PrefixTreeNode* root, std::string_view sv) {
        PrefixTreeNode* node = root;
        for (const char c : sv) {
            size_t idx = charToIndex(c);
            if (!node->_children[idx]) {
                node->_children[idx] = std::make_unique<PrefixTreeNode>(c);
            }
            node = node->_children[idx].get();
        }
        node->_isComplete = true;
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


const std::string help = "";

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
        else std::cout << "unkown cmd" << std::endl;
    }

}

