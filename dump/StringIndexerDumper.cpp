#include "StringIndexerDumper.h"

#include <cstddef>
#include <cstdint>
#include <memory>

#include "AlignedBuffer.h"
#include "DumpConfig.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "GraphDumpHelper.h"
#include "ID.h"
#include "TuringException.h"
#include "indexes/StringIndex.h"
#include "StringIndexerDumpConstants.h"
#include "spdlog/spdlog.h"

using namespace db;

DumpResult<void> StringIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    GraphDumpHelper::writeFileHeader(_writer);
    // Metadata
    _writer.writeToCurrentPage(idxer.size());
    _writer.nextPage();

    for (const auto& [propId, idx] : idxer) {
        ensureSpace(sizeof(uint16_t));
        _writer.writeToCurrentPage(static_cast<uint16_t>(propId.getValue()));
        dumpIndex(idx);
    }

    _writer.finish();

    if (_writer.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS, _writer.error().value());
    }

    return {};
}

DumpResult<void> StringIndexerDumper::dumpIndex(const std::unique_ptr<StringIndex>& idx) {
    _writer.writeToCurrentPage(idx->getNodeCount());
    for (size_t i = 0; i < idx->getNodeCount(); i++) {
        dumpNode(idx->getNode(i));
    }

    return {};
}

DumpResult<void> StringIndexerDumper::dumpNode(const StringIndex::PrefixTreeNode* node) {
    _writer.writeToCurrentPage(node->getID());

    auto& children = node->getChildren();

    size_t nonNullChildren =
        std::ranges::count_if(children, [](auto ptr) { return ptr != nullptr; });
    _writer.writeToCurrentPage(nonNullChildren);

    // Write existing children IDs and their index into the child array
    for (size_t i = 0; i < StringIndex::PrefixTreeNode::ALPHABET_SIZE; i++) {
        StringIndex::PrefixTreeNode* child = node->getChild(i);
        if (child) {
            _writer.writeToCurrentPage(i);
            _writer.writeToCurrentPage(child->getID());
        }
    }

    _writer.writeToCurrentPage(node->getOwners().size());
    dumpOwners(node->getOwners());
    return {};
}

DumpResult<void> StringIndexerDumper::dumpOwners(const std::vector<EntityID>& owners) {
    for (size_t i = 0; i < owners.size(); i++) {
        uint64_t id = owners[i].getValue();
        _auxWriter.writeToCurrentPage(id);
    }
    return {};
}

bool StringIndexerDumper::ensureSpace(size_t requiredSpace) {
    if (requiredSpace > DumpConfig::PAGE_SIZE) {
        return false;
    }
    if (_writer.buffer().avail() < requiredSpace) {
        spdlog::warn("Starting a new page because of ensureSpace({})", requiredSpace);
        _writer.nextPage();
    }
    return true;
}
