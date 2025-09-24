#include "StringIndexerDumper.h"

#include <cstddef>
#include <memory>

#include "DumpUtils.h"
#include "DumpResult.h"
#include "FilePageWriter.h"
#include "GraphDumpHelper.h"
#include "ID.h"
#include "indexes/StringIndex.h"
#include "StringIndexerDumpConstants.h"

using namespace db;

DumpResult<void> StringIndexerDumper::dump(const StringPropertyIndexer& idxer) {
    // 1. Header
    GraphDumpHelper::writeFileHeader(_writer);
    // 2. Metadata
    _writer.writeToCurrentPage(idxer.size());
    _writer.nextPage();

    // 3. Write each index
    for (const auto& [propId, idx] : idxer) {
        DumpUtils::ensureDumpSpace(sizeof(PropertyTypeID::Type), _writer);
        _writer.writeToCurrentPage(static_cast<PropertyTypeID::Type>(propId.getValue()));
        dumpIndex(idx);
    }

    _writer.finish();

    if (_writer.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_WRITE_PROPS,
                                 _writer.error().value());
    }

    return {};
}

DumpResult<void> StringIndexerDumper::dumpIndex(const std::unique_ptr<StringIndex>& idx) {
    DumpUtils::ensureDumpSpace(sizeof(size_t), _writer);
    const size_t size = idx->getNodeCount();

    // 1.  Number of nodes in this index prefix tree
    _writer.writeToCurrentPage(size);

    // 2. Dump each node
    for (size_t i = 0; i < size; i++) {
        if (auto nodeResult = dumpNode(idx->getNode(i)); !nodeResult ) {
            return nodeResult.get_unexpected();
        }
    }

    return {};
}

DumpResult<void> StringIndexerDumper::dumpNode(const StringIndex::PrefixTreeNode* node) {
    DumpUtils::ensureDumpSpace(StringIndexDumpConstants::MAXNODESIZE, _writer);
    // 1. Write internal node data
    { // Space written in this block is accounted for by above call to @ref ensureDumpSpace
        const auto& children = node->getChildren();
        const size_t nonNullChildren =
            std::ranges::count_if(children, [](auto ptr) { return ptr != nullptr; });

        _writer.writeToCurrentPage(node->getID());
        _writer.writeToCurrentPage(nonNullChildren);

        // Write existing children IDs and their index into the child array
        for (size_t i = 0; i < StringIndex::PrefixTreeNode::ALPHABET_SIZE; i++) {
            if (auto child = node->getChild(i)) {
                _writer.writeToCurrentPage(i);
                _writer.writeToCurrentPage(child->getID());
            }
        }
        _writer.writeToCurrentPage(node->getOwners().size());
    }

    // 2. Write owners to external file
    if (const auto ownersResult = dumpOwners(node->getOwners()); !ownersResult) {
        return ownersResult.get_unexpected();
    }

    return {};
}

DumpResult<void> StringIndexerDumper::dumpOwners(const std::vector<EntityID>& owners) {
    if (owners.size() == 0) {
        return {};
    }

    return DumpUtils::dumpVector(owners, _auxWriter);
}

