#pragma once

#include <memory>

#include "AlignedBuffer.h"
#include "DumpResult.h"
#include "FilePageReader.h"
#include "indexers/StringPropertyIndexer.h"

namespace db {

class StringIndexerLoader {
public:
    explicit StringIndexerLoader(fs::FilePageReader& reader,
                                 fs::FilePageReader& auxReader)
        : _reader(reader),
        _auxReader(auxReader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<StringPropertyIndexer>> load();

private:
    fs::FilePageReader& _reader;
    fs::FilePageReader& _auxReader;

    DumpResult<std::unique_ptr<StringIndex>> loadIndex(fs::AlignedBufferIterator& it,
                                                       fs::AlignedBufferIterator& auxIt);

    DumpResult<void> loadNode(std::unique_ptr<StringIndex>& index,
                              fs::AlignedBufferIterator& it,
                              fs::AlignedBufferIterator& auxIt);

    DumpResult<void> loadOwners(StringIndex::PrefixTreeNode* node, size_t sz,
                                fs::FilePageReader& auxReader,
                                fs::AlignedBufferIterator& auxIt);
};
}
