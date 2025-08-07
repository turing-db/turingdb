#include "StringIndexerComparator.h"
#include "spdlog/spdlog.h"

using namespace db;

bool StringIndexerComparator::indexSame(const StringIndex& a, const StringIndex& b) {
    if (a.getNodeCount() != b.getNodeCount()) {
        spdlog::error("Index a has size {} whilst b has {}", a.getNodeCount(),
                      b.getNodeCount());
        return false;
    }

    for (size_t i = 0; i < a.getNodeCount(); i++) {
        if (!nodeSame(a.getNode(i), b.getNode(i))) {
            return false;
        }
    }

    return true;
}

bool StringIndexerComparator::nodeSame(StringIndex::PrefixTreeNode* a,
                                       StringIndex::PrefixTreeNode* b) {
    if (a->getID() != b->getID()) {
        spdlog::error("Index a has ID {} whilst b has {}", a->getID(), b->getID());
        return false;
    }

    if (a->getChildren().size() != b->getChildren().size()) {
        spdlog::error("Index a has {} children whilst b has {}", a->getChildren().size(),
                      b->getChildren().size());
        return false;
    }

    if (a->getOwners().size() != b->getOwners().size()) {
        spdlog::error("Index a has {} owners whilst b has {}", a->getOwners().size(),
                      b->getOwners().size());
        return false;
    }

    for (size_t i = 0 ; i < a->getOwners().size(); i ++) {
        if (a->getOwners()[i] != b->getOwners()[i]) {
            spdlog::error("Mismatching owners vector");
            return false;
        }
    }

    for (size_t i = 0; i < a->getChildren().size(); i++) {
        if ((!a->getChild(i) && b->getChild(i)) || (a->getChild(i) && !b->getChild(i))) {
            spdlog::error("Null and non-null children at index {}", i);
            return false;
        }

        if (!a->getChild(i) && !b->getChild(i)) {
            continue;
        }

        if (a->getChild(i)->getID() != b->getChild(i)->getID()) {
            spdlog::error("Mismatching child IDs: {} and {}", a->getChild(i)->getID(),
                          b->getChild(i)->getID());

            spdlog::error("Child arrays in question:");
            std::cout << "a: ";
            for (size_t j = 0; j < a->getChildren().size(); j++) {
                if (auto child = a->getChild(j)) {
                    std::cout << "(" << j << ", " << child->getID() << "," << child
                              << ") ";
                }
            }
            std::cout << std::endl;
            std::cout << "b: ";
            for (size_t j = 0; j < b->getChildren().size(); j++) {
                if (auto child = b->getChild(j)) {
                    std::cout << "(" << j << ", " << child->getID() << "," << child
                              << ") ";
                }
            }
            std::cout << std::endl;
            return false;
        }
    }

    return true;
}

bool StringIndexerComparator::same(const StringPropertyIndexer& a,
                                         const StringPropertyIndexer& b) {
    // Same number of properties indexed
    if (a.size() != b.size()) {
        spdlog::error("Indexers are not same size");
        return false;
    }

    auto ita = a.begin();

    for (; ita != a.end(); ita++) {
        auto itb = b.find(ita->first);
        if (itb == b.end()) {
            spdlog::error("A has index for propID {} whilst B does not", ita->first);
            return false;
        }

        // Same strings-owners stored
        if (!indexSame(*ita->second, *itb->second)) {
            return false;
        }
    }

    return true;
}
