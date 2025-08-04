#include "StringApproxIndexerLoader.h"

#include "AlignedBuffer.h"
#include "DumpResult.h"
#include "DumpConfig.h"
#include "indexers/StringPropertyIndexer.h"
#include "StringIndexerDumpConstants.h"
#include "indexes/StringIndex.h"
#include <memory>

using namespace db;

DumpResult<std::unique_ptr<StringPropertyIndexer>> StringApproxIndexerLoader::load() {
    _reader.nextPage();
    _auxReader.nextPage();

    auto it = _reader.begin();
    size_t numIdxs = it.get<size_t>();
    _reader.nextPage();

    auto idxer = std::make_unique<StringPropertyIndexer>();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS,
                                 _reader.error().value());
    }

    it = _reader.begin();

    // Check if we received a full page
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }

    for (size_t i = 0; i < numIdxs; i++) {
        auto propId = it.get<PropertyTypeID::Type>();
        auto newIdx = std::make_unique<StringIndex>(loadNode().get());
        idxer->try_emplace(propId, newIdx);
    }


    return {std::move(idxer)};
}

std::unique_ptr<StringIndex::PrefixTreeNode> StringApproxIndexerLoader::loadNode() {
    if (_reader.getBuffer().avail() < NODESIZE) {
        _reader.nextPage();
    }

    auto it = _reader.begin();

    size_t numOwners = it.get<size_t>();

    // TODO: Cleanup hacky overload of .get
    std::span<const uint8_t> bitspan = it.get(CHILDMASKSIZE);

    char c = it.get<char>();
    bool isComplete = it.get<bool>();

    auto node = std::make_unique<StringIndex::PrefixTreeNode>(c);
    node->_isComplete = isComplete;

    loadOwners(node->_owners, it, numOwners);

    for (size_t i = 0; i < SIGMA; i++) {
        if (bitspan[i / 8] & 1 << (i % 8)) {
            node->_children[i] = loadNode();
        }
    }

    return node;
}

void StringApproxIndexerLoader::loadOwners(std::vector<EntityID>& owners,
                                           fs::AlignedBufferIterator it, size_t sz) {
    if (_auxReader.getBuffer().avail() < sz * sizeof(uint64_t)) {
        _auxReader.nextPage();
        it = _auxReader.begin();
    }
    owners.reserve(sz);
    for (size_t i = 0; i < sz; i++) {
        owners.emplace_back(it.get<uint64_t>());
    }
}
