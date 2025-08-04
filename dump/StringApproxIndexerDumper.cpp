#include "StringApproxIndexerDumper.h"

#include <cstddef>
#include <cstdint>

#include "DumpConfig.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "indexes/StringIndex.h"
#include "StringIndexerDumpConstants.h"


using namespace db;

/*

Dept first traversal


*/

DumpResult<void> StringApproxIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    _writer.nextPage();
    _auxWriter.nextPage();

    // Header metadata
    _writer.writeToCurrentPage(idxer.size());
    _writer.nextPage();

    for (const auto& [propId, idx] : idxer) {
        ensureSpace(sizeof(size_t));
        _writer.writeToCurrentPage(propId.getValue());
        dumpNode(idx->getRootRef().get());
    }

    return {};
}

DumpResult<void> StringApproxIndexerDumper::dumpNode(const StringIndex::PrefixTreeNode* node) {
    if (_writer.buffer().avail() < NODESIZE) {
        _writer.nextPage();
    }

    // Map which children exist
    uint8_t bitmap[CHILDMASKSIZE] = {0};

    for (size_t i = 0; i < SIGMA; i++) {
        if (!node->_children[i]) {
            continue;
        }
        // OR the bit in the byte which corresponds to this child
        bitmap[i / 8] |= (1 << (i % 8));
    }
    auto bitspan = std::span(bitmap);

    _writer.writeToCurrentPage(node->_owners.size());
    dumpOwners(node->_owners); // Writes into different file

    _writer.writeToCurrentPage(bitspan);

    _writer.writeToCurrentPage(node->_val);
    _writer.writeToCurrentPage(node->_isComplete);

    for (size_t i = 0; i < SIGMA; i++) {
        if (node->_children[i]) {
            dumpNode(node->_children[i].get());
        }
    }

    return {};
}

DumpResult<void> StringApproxIndexerDumper::dumpOwners(const std::vector<EntityID>& owners) {
    if (_auxWriter.buffer().avail() < owners.size() * sizeof(uint64_t)) {
        _auxWriter.nextPage();
    }

    // NOTE: Consider converting to span and chunk writing instead of one at a time
    for (const auto& id : owners) {
        _auxWriter.writeToCurrentPage(id.getValue());
    }

    return {};
}

void dumpTrie() {
}

bool StringApproxIndexerDumper::ensureSpace(size_t requiredSpace) {
    if (requiredSpace > DumpConfig::PAGE_SIZE) {
        return false;
    }
    if (_writer.buffer().avail() < requiredSpace) {
        _writer.nextPage();
    }
    return true;
}
