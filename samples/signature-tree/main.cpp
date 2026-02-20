#include <bitset>
#include <memory>
#include <stack>
#include <vector>

#include <spdlog/spdlog.h>

template <typename T>
using rc = std::shared_ptr<T>;

static constexpr size_t BitCount = 4;
static constexpr size_t BitsetCount = 102;
static constexpr size_t QueryCount = 24;

using Signature = std::bitset<BitCount>;

struct SignatureNode {
    size_t _bit {0};
    rc<SignatureNode> _left {nullptr};
    rc<SignatureNode> _right {nullptr};

    const Signature* _signature {nullptr};
};

static bool insertSignature(rc<SignatureNode>& root, const Signature& signature) {
    std::stack<rc<SignatureNode>> stack;
    stack.push(root);

    static constexpr auto getDisagreement = [](const Signature& a, const Signature& b) {
        for (size_t i = 0; i < BitCount; i++) {
            if (a.test(i) != b.test(i)) {
                return i;
            }
        }

        return BitCount;
    };

    while (!stack.empty()) {
        const rc<SignatureNode> v = stack.top();
        stack.pop();

        if (v->_signature == nullptr) {
            // Not a leaf
            stack.push(signature.test(v->_bit)
                           ? v->_right
                           : v->_left);
        } else {
            // Leaf
            const Signature* w = v->_signature;

            const size_t k = getDisagreement(signature, *w);

            if (k == BitCount) {
                // Signature already present, not inserting
                return false;
            }

            v->_bit = k;
            v->_signature = nullptr;

            v->_left = std::make_shared<SignatureNode>();
            v->_right = std::make_shared<SignatureNode>();

            if (!signature.test(k)) {
                v->_left->_signature = &signature;
                v->_right->_signature = w;
            } else {
                v->_left->_signature = w;
                v->_right->_signature = &signature;
            }
        }
    }

    return true;
}

[[maybe_unused]] static void printTree(const rc<SignatureNode>& root) {
    std::stack<std::pair<size_t, rc<SignatureNode>>> stack;
    stack.push({0, root});

    while (!stack.empty()) {
        const auto [i, v] = stack.top();
        stack.pop();

        if (v->_signature == nullptr) {
            // Not a leaf
            fmt::print("{}{}\n", std::string(i, '|'), v->_bit);

            stack.push({i + 1, v->_right});
            stack.push({i + 1, v->_left});
        } else {
            fmt::print("{} -{}\n", std::string(i, '|'), v->_signature->to_string());
        }
    }
}

class SignatureTreeIterator {
public:
    SignatureTreeIterator(const rc<SignatureNode>& root, const Signature& signature)
        : _signature(signature) {
        _stack.push(root);
        next();
    }

    void next() {
        _current = nullptr;

        while (!_stack.empty()) {
            rc<SignatureNode> node = _stack.top();
            _stack.pop();

            if (node->_signature != nullptr) {
                // Checking if leaf bits are a match to the signature
                _bitCompareCount += BitCount;
                if ((_signature & *node->_signature) == _signature) {
                    _current = node->_signature;
                    return;
                }
                continue;
            }

            // Not a leaf
            _bitCompareCount++;
            if (_signature.test(node->_bit)) {
                _stack.push(node->_right);
            } else {
                _stack.push(node->_left);
                _stack.push(node->_right);
            }
        }
    }

    bool isValid() const {
        return _current != nullptr;
    }

    const Signature& get() const {
        return *_current;
    }

    size_t getBitCompareCount() const {
        return _bitCompareCount;
    }

private:
    std::stack<rc<SignatureNode>> _stack;
    const Signature& _signature;
    const Signature* _current {nullptr};
    size_t _bitCompareCount {0};
};

int main(int argc, const char** argv) {
    std::srand(0);

    std::vector<std::bitset<BitCount>> signatures(BitsetCount);

    fmt::print("Building signatures\n");
    for (auto& set : signatures) {
        // Randomly set the bits
        for (size_t i = 0; i < BitCount; i++) {
            set[i] = rand() % 2;
        }
    }

    // Initialize tree to be a single leaf pointing to first element
    auto root = std::make_shared<SignatureNode>();
    auto& signature = signatures.front();
    root->_signature = &signature;
    root->_bit = 1;
    fmt::print("- Inserted {}\n", signature.to_string());

    // Add the rest of the bitsets to the tree
    size_t treeSize = 1;
    for (auto& signature : signatures) {
        if (insertSignature(root, signature)) {
            fmt::print("- Inserted {}\n", signature.to_string());
            treeSize++;
        }
    }
    fmt::print("Tree size: {} ({} bits)\n", treeSize, treeSize * BitCount);

    fmt::print("Tree:\n");
    printTree(root);

    {
        std::vector<std::bitset<BitCount>> queries(QueryCount);
        for (auto& query : queries) {
            // Randomly set the bits
            for (size_t i = 0; i < BitCount; i++) {
                query[i] = rand() % 2;
            }
        }

        for (auto& query : queries) {
            SignatureTreeIterator it(root, query);
            fmt::print("- Query: {}\n", query.to_string());
            while (it.isValid()) {
                fmt::print("   * {} matches\n", it.get().to_string());
                it.next();
            }
            fmt::print("  - Complete in {} bit checks\n",
                       it.getBitCompareCount());
        }
    }

    return 0;
}
