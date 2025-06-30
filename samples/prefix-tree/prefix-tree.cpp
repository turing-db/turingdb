#include <cctype>
#include <iostream>
#include <array>
#include <vector>

#include "utils.h"


using namespace std;

// Size of our alphabet: assumes some preprocessing,
// so only termination char and a-z and 1-10
constexpr size_t SIGMA = 1 + 26 + 10;

class StringApproximatorIndex {
public:
    struct prefixTreeNode {
        array<prefixTreeNode*, SIGMA> children;
        // Termination char denotes terminal node
        char val;
        bool isTerminal;

        prefixTreeNode(char val) : children{}, val{val}, isTerminal(false)
        {
        }

    };


    StringApproximatorIndex() {
        _root = new prefixTreeNode{'\1'};
    }

    void insert(string_view str) { return insert(_root, str); }
    void print() { _printTree(_root); }


private:
    prefixTreeNode* _root;

    size_t charToIndex(char c) {
        // Children array layout:
        // INDEX CHARACTER VALUE
        // 0     \0
        // 1     a
        // ...  ...
        // 26    z
        // 27    0
        // ...  ...
        // 37    9
        if (isalpha(c)) return 1 + c - 'a';
        else return 1 + 26 + c - '0';
    }

    void insert(prefixTreeNode* parent, string_view str) {
        if (str.empty()) {
            parent->isTerminal = true;
            return;
        }
        // Get first char
        char c = str.at(0);
        str.remove_prefix(1);
        // Index into the children array
        size_t idx = charToIndex(c);

        // If next char already exists in the tree
        if (parent->children[idx] != nullptr) {
            return insert(parent->children[idx], str);
        }
        else {
            auto newChild = parent->children[idx] = new prefixTreeNode{c};
            return insert(newChild, str);
        }
    }

   void _printTree(prefixTreeNode* node)
    {
        _printTree(node, "", true);
    }

    void _printTree(prefixTreeNode* node,
                    const std::string& prefix,
                    bool isLastChild)
    {
        if (!node) return;

        if (node->val != '\1') {
            std::cout << prefix
                      << (isLastChild ? "└── " : "├── ")
                      << node->val
                      << (node->isTerminal ? "*" : "")
                      << '\n';
        }

        // Gather existing children so we know which one is the last
        vector<prefixTreeNode*> kids;
        kids.reserve(SIGMA);
        for (std::size_t i = 0; i < SIGMA; ++i)
            if (node->children[i]) kids.push_back(node->children[i]);

        // Prefix extension: keep vertical bar if this isn’t last
        std::string nextPrefix = prefix;
        if (node->val != '\1')          // don’t add for sentinel root
            nextPrefix += (isLastChild ? "    " : "│   ");

        // Recurse over children
        for (std::size_t k = 0; k < kids.size(); ++k) {
            _printTree(kids[k], nextPrefix, k + 1 == kids.size());
        }
    }
};


const string help = "";

int main() {
    cout << help << endl;

    StringApproximatorIndex tree{};

    string in;
    while (true) {
        cout << ">";
        getline(cin, in);
        auto tokens = split(in, " ");
        if (tokens[0] == "i") {
            if (tokens.size() != 2) cout << "i <param>" << endl;
            else tree.insert(tokens[1]);
        }
        else if (tokens[0] == "p") tree.print();
        else cout << "unkown cmd" << endl;
        
    }

}

