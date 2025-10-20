#pragma once

#include "FilePageReader.h"
#include "dump/DumpResult.h"

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
