#include "StringApproxIndexerLoader.h"

#include <memory>
#include <stdexcept>

#include "AlignedBuffer.h"
#include "BioAssert.h"
#include "DumpResult.h"
#include "DumpConfig.h"
#include "FilePageReader.h"
#include "indexers/StringPropertyIndexer.h"
#include "StringIndexerDumpConstants.h"
#include "indexes/StringIndex.h"
#include "GraphDumpHelper.h"
#include "spdlog/spdlog.h"

using namespace db;

namespace {
    void managePagesForNodes(fs::FilePageReader& rd, fs::AlignedBufferIterator& it) {
        if (it.remainingBytes() < StringIndexDumpConstants::NODESIZE) {
            spdlog::info("{} space left in buffer yet node is size {}",
                         rd.getBuffer().avail(), StringIndexDumpConstants::NODESIZE);

            rd.nextPage();
            if (rd.errorOccured()) {
                throw std::runtime_error(rd.error().value().fmtMessage());
            }

            it = rd.begin();
            if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
                spdlog::error("It got {} but it should have {}", it.remainingBytes(),
                              DumpConfig::PAGE_SIZE);
                // throw std::runtime_error("it did not get full page");
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

    // Check file header
    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
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
        spdlog::error("Got {} bytes but needed {} bytes", it.remainingBytes(),
                      DumpConfig::PAGE_SIZE);
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }

    auto idxer = std::make_unique<StringPropertyIndexer>();

    for (size_t i = 0; i < numIdxs; i++) {
        auto propId = it.get<uint16_t>();
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
    bool isComplete = it.get<uint8_t>() != 0;
    size_t numOwners = it.get<size_t>();
    // TODO: Cleanup hacky overload of .get
    auto bitspan = it.get(StringIndexDumpConstants::CHILDMASKSIZE);
    spdlog::info("Read node: c={}, compl={}, owners={}", c, isComplete, numOwners);

    auto node = std::make_unique<StringIndex::PrefixTreeNode>(c);
    node->_isComplete = isComplete;

    loadOwners(node->_owners, auxIt, numOwners);

    msgbioassert(bitspan.size() == StringIndexDumpConstants::CHILDMASKSIZE,
                 "Loaded span was incorrect size");
    for (uint8_t byte : bitspan) {
        std::printf("%02x ", byte);
    }
    std::printf("\n");

    for (size_t i = 0; i < StringIndex::ALPHABET_SIZE; i++) {
        if (bitspan[i / 8] & 1 << (i % 8)) {
            char c = i > 25 ? '0' + i - 26 : 'a' + i;
            spdlog::info("Recursively loading {}th child ({})", i, c);
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
