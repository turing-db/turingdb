#include "StringIndex.h"
#include "ID.h"
#include "TuringException.h"

#include <memory>
#include <deque>
#include <string>
#include <unordered_set>

using namespace db;

using PrefixTreeNode = StringIndex::PrefixTreeNode;

StringIndex::StringIndex()
    : _nextFreeID(0),
    _root(PrefixTreeNode::create(*this))
{
}

StringIndex::StringIndex(size_t nodeCount)
    : _nextFreeID(0),
    _root(PrefixTreeNode::create((*this)))
{
    _nodeManager.reserve(nodeCount - 1);
    // sz - 1 nodes since we have root already.
    for (size_t i = 0 ; i < nodeCount - 1; i++) {
        PrefixTreeNode::create(*this);
    }
}

void PrefixTreeNode::setChild(PrefixTreeNode* child, size_t idx) {
    if (idx > ALPHABET_SIZE) [[unlikely]] {
        throw TuringException("Queried child at index" + std::to_string(idx)
                              + " which is out of range (max: "
                              + std::to_string(ALPHABET_SIZE) + ")");
    }
    _children[idx] = child;
}

void PrefixTreeNode::setChild(PrefixTreeNode* child, char c) {
    // @ref charToIndex handles bounds checking
    _children[PrefixTreeNode::charToIndex(c)] = child;
}

PrefixTreeNode* PrefixTreeNode::getChild(char c) const {
    // @ref charToIndex handles bounds checking
    return _children[PrefixTreeNode::charToIndex(c)];
}

PrefixTreeNode* PrefixTreeNode::getChild(size_t idx) const {
    if (idx > ALPHABET_SIZE) [[unlikely]] {
        throw TuringException("Queried child at index" + std::to_string(idx)
                              + " which is out of range (max: "
                              + std::to_string(ALPHABET_SIZE) + ")");
    }
    return _children[idx];
}

PrefixTreeNode* PrefixTreeNode::create(StringIndex& idx) {
    auto node = std::make_unique<PrefixTreeNode>(idx.allocateID());
    PrefixTreeNode* raw = node.get();

    idx.addNode(std::move(node));

    return raw;
}

// NOTE: Better to do lookup table?
size_t PrefixTreeNode::charToIndex(char c) {
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
    if (isalpha(c)) {
        return std::tolower(c, std::locale()) - FIRST_ALPHA_CHAR;
    } else if (isdigit(c)) {
        return NUM_ALPHABETICAL_CHARS + c - FIRST_NUMERAL;
    } else {
        throw TuringException("Invalid character: '" + std::string(1, c) + "' ("
                              + std::to_string(static_cast<int>(c)) + ")");
    }
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

void StringIndex::split(std::vector<std::string>& res, std::string_view str,
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
    if (str.empty()) {
        return;
    }

    PrefixTreeNode* node = _root;

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

    PrefixTreeNode* node = _root;
    if (!node) [[unlikely]] {
        throw TuringException("Could not get root of string indexer in find");
    }

    for (const char c : sv) {
        const size_t idx = PrefixTreeNode::charToIndex(c);
        if (!node->getChild(idx)) {
            return StringIndexIterator {nullptr, NOT_FOUND};
        }
        node = node->getChild(idx);
    }
    const FindResult res = node->isComplete() ? FOUND : FOUND_PREFIX;
    return StringIndexIterator{node, res};
}

void StringIndex::print(std::ostream& out) const {
    printTree(_root, -1, "", false, out);
}

void StringIndex::printTree(PrefixTreeNode* node, ssize_t idx, const std::string& prefix,
                            bool isLastChild, std::ostream& out) const {
    if (!node) return;

    if (idx != -1) {
        out << prefix
                  << (isLastChild ? "└── " : "├── ")
                  << PrefixTreeNode::indexToChar(idx)
                  << (node->isComplete() ? "*" : "")
                  << '\n';
    }

    // Gather existing children so we know which one is the last
    std::vector<std::pair<PrefixTreeNode*, size_t>> kids;
    kids.reserve(PrefixTreeNode::ALPHABET_SIZE);
    for (std::size_t i = 0; i < PrefixTreeNode::ALPHABET_SIZE; i++) {
        if (auto child = node->getChild(i)) {
            kids.push_back({child, i});
        }
    }

    // Prefix extension: keep vertical bar if this isn’t last
    std::string nextPrefix = prefix;
    if (idx != -1)          // don’t add for sentinel root
        nextPrefix += (isLastChild ? "    " : "│   ");

    // Recurse over children
    for (std::size_t k = 0; k < kids.size(); k++) {
        auto [pointer, index] = kids[k];
        printTree(pointer, index, nextPrefix, k + 1 == kids.size(), out);
    }
}

PrefixTreeNode* StringIndex::getPrefixThreshold(std::string_view query) const {
    if (query.empty()) {
        return nullptr;
    }

    PrefixTreeNode* node = _root;
    if (!node) [[unlikely]] {
        throw TuringException(
            "Could not get root of string indexer in getPrefixThreshold");
    }

    // Calculate the minimum length property string we consider a match
    const size_t minPrefixLength = _prefixThreshold * query.size();
    PrefixTreeNode* thresholdPoint {nullptr};

    for (size_t i = 0; const char c : query) {
        const size_t idx = PrefixTreeNode::charToIndex(c);
        if (!node->getChild(idx)) {
            return thresholdPoint;
        }
        // Return the earliest point in the tree at which we find a matching prefix of
        // sufficient length
        if (i >= minPrefixLength) {
            thresholdPoint = node;
            return thresholdPoint;
        }
        node = node->getChild(idx);
        i++;
    }
    return thresholdPoint;
}

size_t StringIndex::allocateID() {
    const size_t id = _nextFreeID;
    _nextFreeID++;
    return id;
}

void StringIndex::addNode(std::unique_ptr<PrefixTreeNode>&& node) {
    _nodeManager.emplace_back(std::move(node));
}
