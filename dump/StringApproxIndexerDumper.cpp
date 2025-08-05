#include "StringApproxIndexerDumper.h"

#include <bitset>
#include <cstddef>
#include <cstdint>

#include "DumpConfig.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "ID.h"
#include "indexes/StringIndex.h"
#include "StringIndexerDumpConstants.h"
#include "spdlog/spdlog.h"


using namespace db;

DumpResult<void> StringApproxIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    // Header metadata
    _writer.writeToCurrentPage(idxer.size());
    _writer.nextPage();

    for (const auto& [propId, idx] : idxer) {
        ensureSpace(sizeof(PropertyTypeID::Type));
        _writer.writeToCurrentPage(propId.getValue());
        dumpNode(idx->getRootRef().get());
    }

    return {};
}

DumpResult<void> StringApproxIndexerDumper::dumpNode(const StringIndex::PrefixTreeNode* node) {
    if (_writer.buffer().avail() < NODESIZE) {
        _writer.nextPage();
        spdlog::warn("Started new page to write node");
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
    // for (uint8_t byte : bitspan) {
    //     std::printf("%02x ", byte);
    // }
    // std::printf("\n");

    _writer.writeToCurrentPage(node->_val);
    _writer.writeToCurrentPage(node->_isComplete);

    _writer.writeToCurrentPage(node->_owners.size());
    spdlog::info("wrote node: c={}, compl={}, owners={}", node->_val, node->_isComplete,
              node->_owners.size());

    // dumpOwners(node->_owners); // Writes into different file

    _writer.writeToCurrentPage(bitspan);


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
        spdlog::warn("Starting a new page because of ensureSpace({})", requiredSpace);
        _writer.nextPage();
    }
    return true;
}
