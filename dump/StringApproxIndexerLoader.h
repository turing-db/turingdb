#pragma once

#include "DumpResult.h"
#include "FilePageReader.h"
#include "indexers/StringPropertyIndexer.h"
#include <memory>

namespace db {

class StringApproxIndexerLoader {
public:

    explicit StringApproxIndexerLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<std::unique_ptr<StringPropertyIndexer>> load();

private:
    fs::FilePageReader& _reader;
};
}
