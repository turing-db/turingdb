#include "StringApproxIndexerLoader.h"

#include "AlignedBuffer.h"
#include "BioAssert.h"
#include "DumpResult.h"
#include "DumpConfig.h"
#include "FilePageReader.h"
#include "indexers/StringPropertyIndexer.h"
#include "StringIndexerDumpConstants.h"
#include "indexes/StringIndex.h"
#include "spdlog/spdlog.h"
#include <memory>
#include <stdexcept>

using namespace db;

namespace {
    void managePagesForNodes(fs::FilePageReader& rd, fs::AlignedBufferIterator& it) {
        if (rd.getBuffer().avail() < NODESIZE) {
            spdlog::info("{} space left in buffer yet node is size {}",
                         rd.getBuffer().avail(), NODESIZE);

            rd.nextPage();
            if (rd.errorOccured()) {
                throw std::runtime_error(rd.error().value().fmtMessage());
            }

            it = rd.begin();
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                throw std::runtime_error("it did not get full page");
            }

            spdlog::warn("Started new page to read node");
        }
    }
}

DumpResult<std::unique_ptr<StringPropertyIndexer>> StringApproxIndexerLoader::load() {
    _reader.nextPage();
    _auxReader.nextPage();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS,
                                 _reader.error().value());
    }

    if (_auxReader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS,
                                 _auxReader.error().value());
    }

    auto it = _reader.begin();
    auto auxIt = _auxReader.begin();
    // Check if we received a full page
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }
    size_t numIdxs = it.get<size_t>();
    _reader.nextPage();
    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS,
                                 _reader.error().value());
    }

    it = _reader.begin();
    // Check if we received a full page
    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        spdlog::error("Iterator did not get full page prior to loading nodes");
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }

    auto idxer = std::make_unique<StringPropertyIndexer>();

    for (size_t i = 0; i < numIdxs; i++) {
        auto propId = it.get<PropertyTypeID::Type>();
        spdlog::info("Read propid {}", propId);
        auto newIdx = std::make_unique<StringIndex>(loadNode(it, auxIt).get());
        bool res = idxer->try_emplace(propId, newIdx);
        if (!res) {
            spdlog::error("Could not emplace index at property id {}", propId);
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }
    }

    return {std::move(idxer)};
}

std::unique_ptr<StringIndex::PrefixTreeNode>
StringApproxIndexerLoader::loadNode(fs::AlignedBufferIterator& it,
                                    fs::AlignedBufferIterator& auxIt) {
    managePagesForNodes(_reader, it);

    char c = it.get<char>();

    auto node = std::make_unique<StringIndex::PrefixTreeNode>(c);

    bool isComplete = it.get<bool>();
    node->_isComplete = isComplete;

    size_t numOwners = it.get<size_t>();
    spdlog::info("Read node: c={}, compl={}, owners={}", c, isComplete, numOwners);
    // loadOwners(node->_owners, auxIt, numOwners);

    // TODO: Cleanup hacky overload of .get
    std::span<const uint8_t> bitspan = it.get(CHILDMASKSIZE);
    msgbioassert(bitspan.size() == CHILDMASKSIZE, "Loaded span was incorrect size");
    // for (uint8_t byte : bitspan) {
    //     std::printf("%02x ", byte);
    // }
    // std::printf("\n");

    for (size_t i = 0; i < SIGMA; i++) {
        if (bitspan[i / 8] & 1 << (i % 8)) {
            node->_children[i] = loadNode(it, auxIt);
        }
    }

    return node;
}

void StringApproxIndexerLoader::loadOwners(std::vector<EntityID>& owners,
                                           fs::AlignedBufferIterator& it, size_t sz) {
    if (_auxReader.getBuffer().avail() < sz * sizeof(uint64_t)) {
        _auxReader.nextPage();
        it = _auxReader.begin();
    }
    owners.reserve(sz);
    for (size_t i = 0; i < sz; i++) {
        owners.emplace_back(it.get<uint64_t>());
    }
}
