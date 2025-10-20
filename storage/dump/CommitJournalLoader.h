#pragma once

#include "dump/DumpResult.h"

namespace fs {
class FilePageReader;
}

namespace db {

class CommitJournal;

class CommitJournalLoader {
public:
    explicit CommitJournalLoader(fs::FilePageReader& reader)
        : _reader(reader)
    {
    }

    [[nodiscard]] DumpResult<void> load(CommitJournal& journal);

private:
    fs::FilePageReader& _reader;
};

}
