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

    StringApproximatorIndex();
    
    void insert(std::string_view str);

    PrefixTreeIterator find(std::string_view str);

    void print() const { _printTree(_root.get()); }

    std::vector<NodeID> getOwners(std::string_view str);

private:
    NodeID _currentID;

    std::unique_ptr<PrefixTreeNode> _root{};

    static size_t charToIndex(char c);

    void _insert(PrefixTreeNode* root, std::string_view sv, NodeID owner);

    PrefixTreeIterator _find(PrefixTreeNode* root, std::string_view sv);

    static std::vector<NodeID> getOwners(PrefixTreeIterator it);

    void _printTree(PrefixTreeNode* node) const;

    void _printTree(PrefixTreeNode* node,
                    const std::string& prefix,
                    bool isLastChild) const;
};
