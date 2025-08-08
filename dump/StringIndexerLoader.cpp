#include "StringIndexerLoader.h"

#include <cstdint>
#include "spdlog/spdlog.h"
#include <memory>

#include "AlignedBuffer.h"
#include "DumpResult.h"
#include "DumpConfig.h"
#include "FilePageReader.h"
#include "TuringException.h"
#include "indexers/StringPropertyIndexer.h"
#include "StringIndexerDumpConstants.h"
#include "indexes/StringIndex.h"
#include "GraphDumpHelper.h"

using namespace db;

namespace {

void ensureIteratorReadPage(fs::AlignedBufferIterator& it) {
    // Check if we received a full page
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        spdlog::error("Iterator did not get full page prior to loading nodes");
        spdlog::error("Got {} bytes but needed {} bytes", it.remainingBytes(),
                      DumpConfig::PAGE_SIZE);
        throw TuringException("Failed to read from file");
    }
}

}

void StringIndexerLoader::ensureSpace(size_t requiredReadSpace,
                                      fs::AlignedBufferIterator& it,
                                      fs::FilePageReader& rd) {
    if (it.remainingBytes() < requiredReadSpace) {
        rd.nextPage();
        it = rd.begin();
        ensureIteratorReadPage(it);
    }
}

DumpResult<std::unique_ptr<StringPropertyIndexer>> StringIndexerLoader::load() {
    _reader.nextPage();
    _auxReader.nextPage();

    auto it = _reader.begin();
    ensureIteratorReadPage(it);

    // 1. Header
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        spdlog::error("Failed to read header");
        return res.get_unexpected();
    }

    // 2. Metadata
    ensureSpace(sizeof(size_t), it, _reader);
    size_t numIndexes = it.get<size_t>();
    _reader.nextPage();

    auto idxer = std::make_unique<StringPropertyIndexer>();

    // If no indexes, nothing more to read, return empty index
    if (numIndexes == 0) {
        idxer->setInitialised();
        return {std::move(idxer)};
    }

    // Refresh the main iterator as we just turned the page from header page
    it = _reader.begin();
    ensureIteratorReadPage(it);

    // Only start the secondary reader after we check that the index is non-empty,
    // otherwise there is no owners page and this throws an error
    auto auxIt = _auxReader.begin();
    ensureIteratorReadPage(auxIt);

    // 3. Read each index
    for (size_t i = 0; i < numIndexes; i++) {
        // PropertyID that this index is indexing
        ensureSpace(sizeof(uint16_t), it, _reader);
        const auto propId = it.get<uint16_t>();

        auto loadResult = loadIndex(it, auxIt);
        if (!loadResult) {
            spdlog::error("Could not load index at property id {}", propId);
            return loadResult.get_unexpected();
        }

        const bool res = idxer->addIndex(propId, std::move(loadResult.value()));
        if (!res) {
            spdlog::error("Could not emplace index at property id {}", propId);
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }
    }

    idxer->setInitialised();

    return {std::move(idxer)};
}

DumpResult<std::unique_ptr<StringIndex>> StringIndexerLoader::loadIndex(fs::AlignedBufferIterator& it,
                                                                        fs::AlignedBufferIterator& auxIt) {
    ensureSpace(sizeof(size_t), it, _reader);
    // 1. Number of nodes in this index prefix tree
    const size_t sz = it.get<size_t>();

    // Create the managing vector of @ref sz unique_pointers to nodes
    auto index = std::make_unique<StringIndex>(sz);

    // 2. Load each node
    for (size_t i = 0; i < sz; i++) {
        if (auto nodeResult = loadNode(index, it, auxIt); !nodeResult) {
            return nodeResult.get_unexpected();
        }
    }

    if (_auxReader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_STR_PROP_INDEXER,
                                 _auxReader.error().value());
    }

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_STR_PROP_INDEXER,
                                 _reader.error().value());
    }

    return {std::move(index)};
}

DumpResult<void> StringIndexerLoader::loadNode(std::unique_ptr<StringIndex>& index,
                                               fs::AlignedBufferIterator& it,
                                               fs::AlignedBufferIterator& auxIt) {
    ensureSpace(StringIndexDumpConstants::MAXNODESIZE, it, _reader);
    // ~~ Managed space starts

    // 1. Write internal node data
    const size_t nodeID = it.get<size_t>();
    StringIndex::PrefixTreeNode* node = index->getNode(nodeID);

    const size_t numChildren = it.get<size_t>();

    // Read children IDs and their index into the child array
    for (size_t i = 0; i < numChildren; i++) {
        const size_t childIndex = it.get<size_t>(); // Index in @ref node's child vector
        const size_t childID = it.get<size_t>();    // Index in @ref index's node vector
        node->setChild(index->getNode(childID), childIndex);
    }

    const size_t numOwners = it.get<size_t>();
    // Managed space ends ~~

    // 2. Read owners from external file
    if (auto ownersResult = loadOwners(node, numOwners, auxIt); !ownersResult) {
        return ownersResult.get_unexpected();
    }

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_STR_PROP_INDEXER,
                                 _reader.error().value());
    }

    return {};
}

DumpResult<void> StringIndexerLoader::loadOwners(StringIndex::PrefixTreeNode* node,
                                                 size_t sz,
                                                 fs::AlignedBufferIterator& auxIt) {
    ensureSpace(sz * sizeof(EntityID::Type), auxIt, _auxReader);
    for (size_t i = 0; i < sz; i++) {
        const uint64_t id = auxIt.get<uint64_t>();
        node->addOwner(id);
    }

    if (_auxReader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_STR_PROP_INDEXER,
                                 _auxReader.error().value());
    }

    return {};
}
