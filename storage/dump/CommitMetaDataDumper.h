#pragma once

#include "DumpResult.h"
#include "FileWriter.h"

namespace db {

class Commit;
class PartMetaDataMap;

class CommitMetaDataDumper {
public:
    [[nodiscard]] static DumpResult<void> dump(const Commit& commit,
                                               fs::FileWriter<>& writer);
};

}
