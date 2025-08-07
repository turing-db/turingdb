#include "StringIndexerLoader.h"

#include <cstdint>
#include <memory>
#include <stdexcept>

#include "AlignedBuffer.h"
#include "BioAssert.h"
#include "DumpResult.h"
#include "DumpConfig.h"
#include "FilePageReader.h"
#include "TuringException.h"
#include "indexers/StringPropertyIndexer.h"
#include "StringIndexerDumpConstants.h"
#include "indexes/StringIndex.h"
#include "GraphDumpHelper.h"
#include "spdlog/spdlog.h"

// TODO: Add new error type for this

using namespace db;

namespace {
    [[maybe_unused]] void managePagesForNodes(fs::FilePageReader& rd, fs::AlignedBufferIterator& it) {
        if (it.remainingBytes() < StringIndexDumpConstants::NODESIZE) {
            spdlog::info("{} space left in buffer yet node is size {}",
                         it.remainingBytes(), StringIndexDumpConstants::NODESIZE);

            rd.nextPage();
            if (rd.errorOccured()) {
                throw std::runtime_error(rd.error().value().fmtMessage());
            }

            it = rd.begin();
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                spdlog::error("It got {} but it should have {}", it.remainingBytes(),
                              DumpConfig::PAGE_SIZE);
                throw std::runtime_error("it did not get full page");
            }
            spdlog::warn("Started new page to read node");
        }
    }
}

DumpResult<std::unique_ptr<StringPropertyIndexer>> StringIndexerLoader::load() {
    _reader.nextPage();
    _auxReader.nextPage();

    auto it = _reader.begin();
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        spdlog::error("Failed to read header");
        return res.get_unexpected();
    }
    size_t numIndexes = it.get<size_t>();
    _reader.nextPage();

    auto idxer = std::make_unique<StringPropertyIndexer>();
    // If no indexes, nothing more to read, return empty index
    if (numIndexes == 0) {
        idxer->setInitialised();
        return {std::move(idxer)};
    }
    it = _reader.begin();
    auto auxIt = _auxReader.begin();

    for (size_t i = 0; i < numIndexes; i++) {
        auto propId = it.get<uint16_t>();

        auto loadResult = loadIndex(it, auxIt);
        if (!loadResult) {
            spdlog::error("Could not load index at property id {}", propId);
            return loadResult.get_unexpected();
        }

        bool res = idxer->addIndex(propId, std::move(loadResult.value()));
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
    // Number of nodes in this index prefix tree
    const size_t sz = it.get<size_t>();

    // Create the managing vector of @ref sz unique_pointers to nodes
    auto index = std::make_unique<StringIndex>(sz);

    // Load each node
    for (size_t i = 0; i < sz; i++) {
        loadNode(index, it, auxIt);
    }

    return {std::move(index)};
}

DumpResult<void> StringIndexerLoader::loadNode(std::unique_ptr<StringIndex>& index,
                                               fs::AlignedBufferIterator& it,
                                               fs::AlignedBufferIterator& auxIt) {
    const size_t nodeID = it.get<size_t>();
    StringIndex::PrefixTreeNode* node = index->getNode(nodeID);

    const size_t numChildren = it.get<size_t>();

    for (size_t i = 0; i < numChildren; i++) {
        const size_t childIndex = it.get<size_t>(); // Index in @ref node's child vector
        const size_t childID = it.get<size_t>();    // Index in @ref index's node vector
        node->setChild(index->getNode(childID), childIndex);
    }

    const size_t numOwners = it.get<size_t>();
    loadOwners(node, numOwners, auxIt);

    return {};
}

DumpResult<void> StringIndexerLoader::loadOwners(StringIndex::PrefixTreeNode* node,
                                                 size_t sz,
                                                 fs::AlignedBufferIterator& auxIt) {
    for (size_t i = 0; i < sz; i++) {
        const uint64_t id = auxIt.get<uint64_t>();
        node->addOwner(id);
    }
    return {};
}
