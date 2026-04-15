#include "HAMTIndex.h"

#include <bit>

#include "ArcManager.h"
#include "FatalException.h"
#include "HAMTManager.h"
#include "HAMTNode.h"

using namespace db;

template <typename K, typename V, typename Hash>
void HAMTIndex<K, V, Hash>::insert(const K& key, const V& value) {
    throw FatalException("Not implemented");

    const size_t hash = _hasher(key);

    WeakArc<HAMTNode<K, V>> node = _root;

    size_t chunkMask = _initialChunkMask;

    // Traverse the tree from root to a leaf
    while (node) {
        const typename HAMTNode<K, V>::ChildBitmask childMap = node->mask();

        // Isolate the section of the hash for lookup
        size_t chunk = hash & chunkMask;
        // Get a bitstring with a single 1 in the position for this child
        const size_t bitMask = 1UL << chunk;

        // If there is a 1 in the bit position, then there is a child
        const bool exists = (bitMask & childMap) != 0;

        if (exists) {
            // Get the number of children of lesser index than the match
            // e.g. if bitMask = 0b1000, chldrnBelowMask = 0b0111 
            const size_t chldrnBelowMask = bitMask - 1;
            // AND the below mask with the actual mask: bit string of lesser children
            const size_t chldrnBelow = childMap & chldrnBelowMask;
            // Get the number of children below: gives the index into the children array
            const size_t numChldrnBelow = std::popcount(chldrnBelow);

            const auto& children = node->children();

            node = children[numChldrnBelow];
        } else {
            WeakArc<Node> newNode = _man->newNode();
        }
    }
}
