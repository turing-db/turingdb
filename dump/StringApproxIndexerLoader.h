#pragma once

#include "DumpResult.h"
#include "FilePageReader.h"
#include "indexers/StringPropertyIndexer.h"
#include <memory>

namespace db {

class StringApproxIndexerLoader {
public:
    explicit StringApproxIndexerLoader(fs::FilePageReader& reader,
                                       fs::FilePageReader& auxReader)
        : _reader(reader),
          _auxReader(auxReader) {}

    [[nodiscard]] DumpResult<std::unique_ptr<StringPropertyIndexer>> load();

private:
    fs::FilePageReader& _reader;
    fs::FilePageReader& _auxReader;

    std::unique_ptr<StringIndex::PrefixTreeNode> loadNode();
    void loadOwners(std::vector<EntityID>& owners, fs::AlignedBufferIterator it,
                    size_t sz);
};
}
