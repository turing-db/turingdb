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
          _auxReader(auxReader) {}

    [[nodiscard]] DumpResult<std::unique_ptr<StringPropertyIndexer>> load();

private:
    fs::FilePageReader& _reader;
    fs::FilePageReader& _auxReader;

    std::unique_ptr<StringIndex::PrefixTreeNode> loadNode(fs::AlignedBufferIterator& it, fs::AlignedBufferIterator& auxIt);

    void loadOwners(std::vector<EntityID>& owners, fs::AlignedBufferIterator& it,
                    size_t sz);
};
}
