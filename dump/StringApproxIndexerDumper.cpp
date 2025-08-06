#include "StringApproxIndexerDumper.h"

#include <cstddef>
#include <cstdint>

#include "DumpConfig.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "GraphDumpHelper.h"
#include "ID.h"
#include "indexes/StringIndex.h"
#include "StringIndexerDumpConstants.h"
#include "spdlog/spdlog.h"

using namespace db;

DumpResult<void> StringApproxIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    GraphDumpHelper::writeFileHeader(_writer);
    // Metadata
    _writer.writeToCurrentPage(idxer.size());
    _writer.nextPage();

    for (const auto& [propId, idx] : idxer) {
        ensureSpace(sizeof(uint16_t));
        _writer.writeToCurrentPage(static_cast<uint16_t>(propId.getValue()));
        dumpNode(idx->getRootRef().get());
    }

    _writer.finish();

    if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS, _writer.error().value());
    }

    return {};
}

DumpResult<void> StringApproxIndexerDumper::dumpNode(const StringIndex::PrefixTreeNode* node) {
    if (_writer.buffer().avail() < StringIndexDumpConstants::NODESIZE) {
        _writer.nextPage();
        spdlog::warn("Started new page to write node");
    }

    // Map which children exist
    uint8_t bitmap[StringIndexDumpConstants::CHILDMASKSIZE] = {0};

    for (size_t i = 0; i < StringIndex::ALPHABET_SIZE; i++) {
        if (!node->_children[i]) {
            continue;
        }
        // OR the bit in the byte which corresponds to this child
        bitmap[i / 8] |= (1 << (i % 8));
    }
    auto bitspan = std::span(bitmap);
    for (uint8_t byte : bitspan) {
        std::printf("%02x ", byte);
    }
    std::printf("\n");

    _writer.writeToCurrentPage(static_cast<char>(node->_val));
    _writer.writeToCurrentPage(static_cast<uint8_t>(node->_isComplete));
    _writer.writeToCurrentPage(static_cast<size_t>(node->_owners.size()));
    _writer.writeToCurrentPage(bitspan);
    spdlog::info("wrote node: c={}, compl={}, owners={}", node->_val, node->_isComplete,
              node->_owners.size());

    dumpOwners(node->_owners); // Writes into different file

    for (size_t i = 0; i < StringIndex::ALPHABET_SIZE; i++) {
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
        spdlog::warn("Writing Owner: {}", id.getValue());
        _auxWriter.writeToCurrentPage(id.getValue());
    }

    return {};
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
