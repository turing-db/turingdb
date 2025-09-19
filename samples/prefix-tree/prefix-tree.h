#pragma once

#include <cstddef>
#include <array>
#include <cstdint>
#include <vector>
#include <memory>

// Size of our alphabet: assumes some preprocessing,
// so only a-z and 1-9
constexpr size_t SIGMA = 26 + 10;

using NodeID = uint32_t;

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
        char _val {'\0'};
        bool _isComplete {false};
        std::unique_ptr<NodeOwners> _owners{};

        PrefixTreeNode(char val)
            : _val{val}
        {
        }
    };

    struct PrefixTreeIterator {
        FindResult _result {NOT_FOUND};
        PrefixTreeNode* _nodePtr {nullptr};
    };

    StringApproximatorIndex();
    
    void insert(std::string_view str, NodeID owner=0);

    const PrefixTreeIterator find(std::string_view sv) const ;

    void print() const;

    void getOwners(std::vector<NodeID>& owners, std::string_view str);

private:
    NodeID _currentID {0};
    std::unique_ptr<PrefixTreeNode> _root{};

    static size_t charToIndex(char c);

    void printTree(PrefixTreeNode* node,
                    const std::string& prefix,
                    bool isLastChild) const;
};
