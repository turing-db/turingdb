#include "StringIndexerLoader.h"

#include <memory>
#include <spdlog/spdlog.h>

#include "ID.h"
#include "AlignedBuffer.h"
#include "DumpResult.h"
#include "FilePageReader.h"
#include "indexers/StringPropertyIndexer.h"
#include "StringIndexerDumpConstants.h"
#include "indexes/StringIndex.h"
#include "GraphDumpHelper.h"
#include "LoadUtils.h"

using namespace db;

DumpResult<std::unique_ptr<StringPropertyIndexer>> StringIndexerLoader::load() {
    _reader.nextPage();
    _auxReader.nextPage();

    auto it = _reader.begin();
    LoadUtils::ensureIteratorReadPage(it);

    // 1. Header
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        spdlog::error("Failed to read header");
        return res.get_unexpected();
    }

    // 2. Metadata
    LoadUtils::ensureLoadSpace(sizeof(size_t), _reader, it);
    const size_t numIndexes = it.get<size_t>();
    _reader.nextPage();

    auto idxer = std::make_unique<StringPropertyIndexer>();

    // If no indexes, nothing more to read, return empty index
    if (numIndexes == 0) {
        idxer->setInitialised();
        return {std::move(idxer)};
    }

    // Refresh the main iterator as we just turned the page from header page
    it = _reader.begin();
    LoadUtils::ensureIteratorReadPage(it);

    // Only start the secondary reader after we check that the index is non-empty,
    // otherwise there is no owners page and this throws an error
    auto auxIt = _auxReader.begin();
    LoadUtils::ensureIteratorReadPage(auxIt);

    // 3. Read each index
    for (size_t i = 0; i < numIndexes; i++) {
        // PropertyID that this index is indexing
        LoadUtils::ensureLoadSpace(sizeof(PropertyTypeID::Type), _reader, it);
        const auto propId = it.get<PropertyTypeID::Type>();

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
    LoadUtils::ensureLoadSpace(sizeof(size_t), _reader, it);
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
    LoadUtils::ensureLoadSpace(StringIndexDumpConstants::MAXNODESIZE, _reader, it);
    // ~~ Managed space starts

    // 1. Write internal node data
    const size_t nodeID = it.get<size_t>(); // ID of the node in index, not @ref NodeID
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
    if (auto ownersResult = loadOwners(node, numOwners, _auxReader, auxIt);
        !ownersResult) {
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
                                                 fs::FilePageReader& auxReader,
                                                 fs::AlignedBufferIterator& auxIt) {
    if (sz == 0) {
        return {};
    }
    std::vector<EntityID>& ownersVec = node->_owners;
    const auto res = LoadUtils::loadVector(ownersVec, sz, auxReader, auxIt);
    return res;
}
