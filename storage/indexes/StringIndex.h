#pragma once

#include <cstddef>
#include <deque>
#include <unordered_set>

#include "ID.h"

// Size of our alphabet: assumes some preprocessing,
// so only a-z and 1-9
constexpr size_t SIGMA = 26 + 10;

namespace db {

/*
 * @brief Approximate string indexing using prefix trees (tries)
 */
class StringIndex {
private:
    struct PrefixTreeNode {
        using NodeOwners = std::vector<EntityID>;

        std::array<std::unique_ptr<PrefixTreeNode>, SIGMA> _children{};
        char _val {'\0'};
        // NOTE: Can we remove isComplete in favour of empty owners?
        bool _isComplete {false};
        std::unique_ptr<NodeOwners> _owners{};

        PrefixTreeNode(char val)
            : _val{val}
        {
        }
    };
    
public:
    enum FindResult {
        FOUND,
        FOUND_PREFIX,
        NOT_FOUND
    };

    using StringIndexNode = PrefixTreeNode;
    struct StringIndexIterator {
        FindResult _result {NOT_FOUND};
        StringIndexNode* _nodePtr {nullptr};
    };

    StringIndex();

    StringIndex(const StringIndex&) = delete;
    StringIndex& operator=(const StringIndex&) = delete;
    StringIndex(StringIndex&&) = delete;

    /**
     * @brief Add a preprocessed word into the index
     * @param str The preprocessed string to insert
     * @param owner The entity ID that owns this string
     * @warning str must be preprocessed using @ref preprocess before calling this
     * function
     */
    void insert(std::string_view str, EntityID owner);

    /**
     * @brief Get the EntityID owners which the query string @param sv matches against
     * @detail Preprocesses the input string, and returns the IDs of any full or prefix
     * matches of any of the tokens in @param sv
     * @param str The string to query
     */
    template <typename T>
    const void query(std::vector<T>& result, std::string_view queryString) const {
        using Node = StringIndexNode;

        //result.clear();
        // Track owners in a set to avoid duplicates
        std::unordered_set<T> resSet;

        std::vector<std::string> tokens {};
        preprocess(tokens, queryString);

        for (const auto& tok : tokens) {
            auto it = find(tok);

            // Early exit if no match
            if (it._result == NOT_FOUND || !it._nodePtr) {
                continue;
            }

            // Otherwise: match or partial match
            Node* node = it._nodePtr;
            std::deque<Node*> q {node};
            // BFS, collecting owners
            while (!q.empty()) {
                const Node* n = q.front();
                q.pop_front();
                for (size_t i {0}; i < n->_children.size(); i++) {
                    if (Node* child = n->_children[i].get()) {
                        q.push_back(child);
                    }
                }
                // Collect owners to report back
                if (n->_isComplete) {
                    std::vector<EntityID>* owners = n->_owners.get();

                    // XXX: nasty
                    for (const EntityID& id : *owners) {
                        resSet.emplace(T(id.getValue()));
                    }
                
                    //resSet.insert(std::begin(*owners), std::end(*owners));
                }
            }
        }
        //result.resize(resSet.size());  // Allocate enough space
        std::copy(resSet.begin(), resSet.end(), std::back_inserter(result));
    }

    /**
     * @brief Replaces punctuation with spaces and splits into words (separated by space)
     * @param in The string to preprocess
     * @param res The resultant tokens of preprocessing @param in
     */
    static void preprocess(std::vector<std::string>& res, const std::string_view in);

private:
    std::unique_ptr<PrefixTreeNode> _root {};

    /**
     * @brief Get an iterator to a preprocessed word existing in the index
     * @param str The preprocessed string to find
     * @warning str must be preprocessed using @ref preprocess before calling this
     * function
     */
    const StringIndexIterator find(std::string_view sv) const;

    static size_t charToIndex(char c);

    static void alphaNumericise(const std::string_view in, std::string& out);

    static void split(std::vector<std::string>& res, std::string_view str,
                      std::string_view delim);
};

}
