#include "StringIndex.h"
#include "ID.h"
#include "TuringException.h"
#include "indexes/StringIndexUtils.h"

#include <memory>
#include <deque>
#include <unordered_set>

using namespace db;

StringIndex::StringIndex()
    : _root(PrefixTreeNode::create(*this))
{
}

void PrefixTreeNode::setChild(PrefixTreeNode* child, char c) {
    _children[StringIndexUtils::charToIndex(c)] = child;
}

PrefixTreeNode* PrefixTreeNode::getChild(char c) const {
    return _children[StringIndexUtils::charToIndex(c)];
}

PrefixTreeNode* PrefixTreeNode::create(StringIndex& idx) {
    auto node = std::make_unique<PrefixTreeNode>();
    PrefixTreeNode* raw = node.get();

    idx.addNode(std::move(node));

    return raw;
}

void StringIndex::alphaNumericise(const std::string_view in, std::string& out) {
    out.clear();
    if (in.empty()) {
        return;
    }

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
    res.clear(); if (str.empty()) { return; }
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
    if (str.empty()) {
        return;
    }

    PrefixTreeNode* node = this->_root.get();

    if (!node) [[unlikely]] {
        throw TuringException("Could not get root of string indexer");
    }

    for (const char c : str) {
        if (!node->getChild(c)) {
            PrefixTreeNode* newChild = PrefixTreeNode::create(*this);
            node->setChild(newChild,c);
        }
        node = node->getChild(c);
    }

    if (!node) [[unlikely]] {
        throw TuringException("Could not get root of string indexer");
    }
    node->addOwner(owner);
}

StringIndex::StringIndexIterator StringIndex::find(std::string_view sv) const {
    if (sv.empty()) [[unlikely]] {
        return {nullptr, NOT_FOUND};
    }

    PrefixTreeNode* node = _root.get();
    if (!node) [[unlikely]] {
        throw TuringException("Could not get root of string indexer in find");
    }

    for (const char c : sv) {
        if (!node->getChild(c)) {
            return StringIndexIterator {nullptr, NOT_FOUND};
        }
        node = node->getChild(c);
    }
    const FindResult res = node->isComplete() ? FOUND : FOUND_PREFIX;
    return StringIndexIterator{node, res};
}

void StringIndex::print(std::ostream& out) const {
    printTree(this->_root.get(), -1, "", false, out);
}

void StringIndex::printTree(PrefixTreeNode* node, size_t idx, const std::string& prefix,
                            bool isLastChild, std::ostream& out) const {
    if (!node) return;

    if (idx != -1) {
        out << prefix
                  << (isLastChild ? "└── " : "├── ")
                  << StringIndexUtils::indexToChar(idx)
                  << (node->isComplete() ? "*" : "")
                  << '\n';
    }

    // Gather existing children so we know which one is the last
    std::vector<std::pair<PrefixTreeNode*, size_t>> kids;
    kids.reserve(SIGMA);
    for (std::size_t i = 0; i < SIGMA; ++i) {
        if (auto child = node->getChild(StringIndexUtils::indexToChar(i))) {
            kids.push_back({child, i});
        }
    }

    // Prefix extension: keep vertical bar if this isn’t last
    std::string nextPrefix = prefix;
    if (idx != -1)          // don’t add for sentinel root
        nextPrefix += (isLastChild ? "    " : "│   ");

    // Recurse over children
    for (std::size_t k = 0; k < kids.size(); ++k) {
        auto [pointer, index] = kids[k];
        printTree(pointer, index, nextPrefix, k + 1 == kids.size());
    }
}

PrefixTreeNode* StringIndex::getPrefixThreshold(std::string_view query) const {
    if (query.empty()) {
        return nullptr;
    }

    PrefixTreeNode* node = _root.get();
    if (!node) [[unlikely]] {
        throw TuringException(
            "Could not get root of string indexer in getPrefixThreshold");
    }

    // Calculate the minimum length property string we consider a match
    const size_t minPrefixLength = _prefixThreshold * query.size();
    PrefixTreeNode* thresholdPoint {nullptr};

    for (size_t i = 0; const char c : query) {
        if (!node->getChild(c)) {
            return thresholdPoint;
        }
        // Return the earliest point in the tree at which we find a matching prefix of
        // sufficient length
        if (i >= minPrefixLength) {
            thresholdPoint = node;
            return thresholdPoint;
        }
        node = node->getChild(c);
        i++;
    }
    return thresholdPoint;
}
