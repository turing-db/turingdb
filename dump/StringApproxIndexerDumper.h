#pragma once

#include "DumpResult.h"
#include "FilePageWriter.h"
#include "indexers/StringPropertyIndexer.h"

namespace db {

class StringApproxIndexerDumper {
public:
    explicit StringApproxIndexerDumper (fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    DumpResult<void> dump(const StringPropertyIndexer& idx);

private:
    fs::FilePageWriter& _writer;

    bool ensureSpace(size_t requiredSpace);
};

}
