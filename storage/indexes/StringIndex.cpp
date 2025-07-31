#include "StringIndex.h"
#include "ID.h"
#include "TuringException.h"

#include <memory>
#include <deque>
#include <unordered_set>
#include <iostream>

using namespace db;

StringIndex::StringIndex()
    : _root(std::make_unique<PrefixTreeNode>('\1'))
{
}

// NOTE: Better to do lookup table?
size_t StringIndex::charToIndex(char c) {
    // Children array layout:
    // INDEX CHARACTER VALUE
    // 0     a
    // ...  ...
    // 25    z
    // 26    0
    // ...  ...
    // 36    9

    // NOTE: Converts upper-case characters to lower to calculate index,
    // but the value of the node is still uppercase
    if (isalpha(c)) return std::tolower(c, std::locale()) - 'a';
    else if (isdigit(c)) return 26 + c - '0';
    else throw TuringException("Invalid character: " + std::to_string(c));
}

void StringIndex::alphaNumericise(const std::string_view in, std::string& out) {
    out.clear();            // Does not deallocate space
    if (in.empty()) return;

    std::locale loc {"C"};  // NOTE: Only support ASCII, remove all else
    auto ppxChar = [&loc](char c) {
        if (!std::isalnum(c, loc)) {
            return ' ';
        } else if (std::isalpha(c)) {
            return std::tolower(c, loc);
        } else {
            return c;
        }
    };

    std::transform(std::begin(in), std::end(in), std::back_inserter(out), ppxChar);
}

void StringIndex::split(std::vector<std::string>& res,
                        std::string_view str,
                        std::string_view delim) {
    res.clear();
    if (str.empty()) {
        return;
    }
    size_t l {0};
    size_t r = str.find(delim);

    while (r != std::string::npos) {
        // Add only if the string is non-empty (removes repeated delims)
        if (l != r) {
            res.push_back(std::string(str.substr(l, r - l)));
        }
        l = r + 1;
        r = str.find(delim, l);
    }

    res.push_back(std::string(str.substr(l, std::min(r, str.size()) - l + 1)));
}


void StringIndex::preprocess(std::vector<std::string>& res, const std::string_view in) {
    std::string cleaned;
    StringIndex::alphaNumericise(in, cleaned);
    split(res, cleaned, " ");
}

void StringIndex::insert(std::string_view str, EntityID owner) {
    if (str.empty()) return;

    PrefixTreeNode* node = this->_root.get();

    if (!node) [[unlikely]] {
        throw TuringException("Could not get root of string indexer");
    }

    for (const char c : str) {
        const size_t idx = charToIndex(c);
        if (!node->_children[idx]) {
            node->_children[idx] = std::make_unique<PrefixTreeNode>(c);
        }
        node = node->_children[idx].get();
    }

    node->_isComplete = true;
    
    node->_owners.push_back(owner);
}

StringIndex::StringIndexIterator StringIndex::find(std::string_view sv) const {
    if (sv.empty()) [[unlikely]] {
        return StringIndexIterator {nullptr, NOT_FOUND};
    }

    PrefixTreeNode* node = this->_root.get();
    if (!node) [[unlikely]] {
        throw TuringException("Could not get root of string indexer");
    }

    for (const char c : sv) {
        const size_t idx = charToIndex(c);
        if (!node->_children[idx]) {
            return StringIndexIterator {nullptr, NOT_FOUND};
        }
        node = node->_children[idx].get();
    }
    const FindResult res = node->_isComplete ? FOUND : FOUND_PREFIX;
    return StringIndexIterator{node, res};
}

void StringIndex::print(std::ostream& out) const {
    printTree(this->_root.get(), "", false, out);
}

void StringIndex::printTree(StringIndex::PrefixTreeNode* node,
                const std::string& prefix,
                bool isLastChild, std::ostream& out) const {
    if (!node) return;

    if (node->_val != '\1') {
        out << prefix
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
