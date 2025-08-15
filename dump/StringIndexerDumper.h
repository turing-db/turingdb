#pragma once

#include <memory>

#include "DumpResult.h"
#include "FilePageWriter.h"
#include "indexers/StringPropertyIndexer.h"

namespace db {

class StringIndexerDumper {
public:
    explicit StringIndexerDumper(fs::FilePageWriter& writer,
                                 fs::FilePageWriter& auxWriter)
        : _writer(writer),
        _auxWriter(auxWriter)
    {
    }

    DumpResult<void> dump(const StringPropertyIndexer& idx);

private:
    fs::FilePageWriter& _writer;
    fs::FilePageWriter& _auxWriter;

    void ensureSpace(size_t requiredSpace, fs::FilePageWriter& wr);

    DumpResult<void> dumpIndex(const std::unique_ptr<StringIndex>& idx);

    DumpResult<void> dumpNode(const StringIndex::PrefixTreeNode* node);
    DumpResult<void> dumpOwners(const std::vector<EntityID>& owners);
};

}
