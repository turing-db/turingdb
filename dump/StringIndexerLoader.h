#pragma once

#include "AlignedBuffer.h"
#include "DumpResult.h"
#include "FilePageReader.h"
#include "indexers/StringPropertyIndexer.h"
#include <memory>

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
                                fs::AlignedBufferIterator& auxIt);

    void ensureSpace(size_t requiredReadSpace, fs::AlignedBufferIterator& it,
                     fs::FilePageReader& rd);
};
}
