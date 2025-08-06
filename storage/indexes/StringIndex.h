#pragma once

#include <cstddef>
#include <deque>
#include <iostream>
#include <memory>
#include <unordered_set>

#include "ID.h"

namespace db {

/*
 * @brief Approximate string indexing using prefix trees (tries)
 * @detail String properties are preprocesed, replacing any non-alphanumeric characters
 * with spaces, and subsequently splitting the string into substrings using spaces as a
 * delimiter. Each substring is then inserted into the trie, associated with the "owner"
 * (NodeID/EdgeID) which has that property value.
 */
class StringIndex {
public:
    class PrefixTreeNode {
    public:
        PrefixTreeNode()
            : _children(ALPHABET_SIZE)
        {
        }

        const std::vector<PrefixTreeNode*>& getChildren() const { return _children; }

        PrefixTreeNode* getChild(char c) const;

        PrefixTreeNode* getChild(size_t idx) const;

        const std::vector<EntityID>& getOwners() const { return _owners; }

        bool isComplete() const { return !_owners.empty(); }

        void setChild(PrefixTreeNode* child, char c);

        void addOwner(EntityID o) { _owners.push_back(o); }

        static PrefixTreeNode* create(StringIndex& idx);

        static inline size_t charToIndex(char c);
        static inline char indexToChar(size_t idx);

    private:
        std::vector<EntityID> _owners;
        std::vector<PrefixTreeNode*> _children;

        static constexpr char FIRST_ALPHA_CHAR = 'a';
        static constexpr char LAST_ALPHA_CHAR = 'z';
        static constexpr char FIRST_NUMERAL = '0';
        static constexpr char LAST_NUMERAL = '9';
        static constexpr size_t NUM_ALPHABETICAL_CHARS =
            LAST_ALPHA_CHAR - FIRST_ALPHA_CHAR + 1;
        static constexpr size_t NUM_NUMERICAL_CHARS = LAST_NUMERAL - FIRST_NUMERAL + 1;

    public:
        static constexpr size_t ALPHABET_SIZE =
            NUM_ALPHABETICAL_CHARS + NUM_NUMERICAL_CHARS;
    };

    enum FindResult {
        FOUND,
        FOUND_PREFIX,
        NOT_FOUND
    };

    struct StringIndexIterator {
        PrefixTreeNode* _nodePtr {nullptr};
        FindResult _result {NOT_FOUND};    
    };

    StringIndex();
    StringIndex(PrefixTreeNode* root)
        : _root(root)
    {
    }
     StringIndex(std::unique_ptr<PrefixTreeNode> root)
        : _root(std::move(root))
    {
    }

    StringIndex(const StringIndex&) = delete;
    StringIndex& operator=(const StringIndex&) = delete;
    StringIndex& operator=(const StringIndex&&) = delete;
    StringIndex(StringIndex&&) = delete;

    /**
     * @brief Add a preprocessed word into the index
     * @param str The preprocessed string to insert
     * @param owner The entity ID that owns this string
     * @warning str must be preprocessed using @ref preprocess before calling this
     * function
     */
    void insert(std::string_view str, EntityID owner);

    void print(std::ostream& out = std::cout) const;

    /**
     * @brief Get the EntityID owners which the query string @param sv matches against
     * @detail Preprocesses the input string, and returns the IDs of any full or prefix
     * matches of any of the tokens in @param sv.
     * @warn Does not empty/clear @param result
     * @param str The string to query
     */
    template <TypedInternalID IDT>
    void query(std::vector<IDT>& result, std::string_view queryString) const;

    /**
     * @brief Replaces punctuation with spaces and splits into words (separated by
     * space)
     * @param in The string to preprocess
     * @param res The resultant tokens of preprocessing @param in
     */
    static void preprocess(std::vector<std::string>& res, std::string_view in);

    auto& getRootRef() const { return _root; }



private:
    std::vector<std::unique_ptr<PrefixTreeNode>> _nodeManager;
    PrefixTreeNode* _root {nullptr};
    static constexpr float _prefixThreshold {0.75};

    /**
     * @brief Get an iterator to a preprocessed word existing in the index
     * @param str The preprocessed string to find
     * @warning str must be preprocessed using @ref preprocess before calling this
     * function
     */
    StringIndexIterator find(std::string_view sv) const;


    /**
     * @brief Implements
     * https://www.notion.so/turingbio/Approximate-String-Matching-21e3aad664c880dba168d3f65a3dac73?source=copy_link#2313aad664c880c78884e97e8da3eed1
     */
    PrefixTreeNode* getPrefixThreshold(std::string_view query) const;

    static void alphaNumericise(const std::string_view in, std::string& out);

    static void split(std::vector<std::string>& res, std::string_view str,
                      std::string_view delim);

    void printTree(PrefixTreeNode* node, ssize_t idx, const std::string& prefix,
                   bool isLastChild, std::ostream& out = std::cout) const;

    void addNode(std::unique_ptr<PrefixTreeNode>&& node) {
        _nodeManager.emplace_back(std::move(node));
    }
};

template <TypedInternalID IDT>
void StringIndex::query(std::vector<IDT>& result, std::string_view queryString) const {
    // Track owners in a set to avoid duplicates
    std::unordered_set<IDT> resSet;
    std::vector<std::string> tokens {};
    preprocess(tokens, queryString);

    for (const auto& tok : tokens) {
        // Find the point in the trie which is deemed a suitable length prefix
        PrefixTreeNode* prefixThreshold = getPrefixThreshold(tok);

        // Early exit if no match
        if (!prefixThreshold) {
            continue;
        }

        // Otherwise: match or partial match
        std::deque<PrefixTreeNode*> q {prefixThreshold};
        // BFS, collecting owners
        // TODO: Depth limit
        while (!q.empty()) {
            const PrefixTreeNode* n = q.front();
            q.pop_front();
            for (size_t i = 0; i < PrefixTreeNode::ALPHABET_SIZE; i++) {
                if (PrefixTreeNode* child = n->getChild(i)) {
                    q.push_back(child);
                }
            }
            // Collect owners to report back
            if (n->isComplete()) {
                auto& owners = n->getOwners();
                for (const EntityID& id : owners) {
                    resSet.emplace(IDT(id.getValue()));
                }
            }
        }
    }
    std::copy(resSet.begin(), resSet.end(), std::back_inserter(result));
}

}
