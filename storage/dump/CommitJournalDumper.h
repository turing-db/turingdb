#pragma once

#include "dump/DumpResult.h"

namespace fs {
class FilePageWriter;
}

namespace db {

class CommitJournal;

class CommitJournalDumper {
public:
    explicit CommitJournalDumper(fs::FilePageWriter& writer)
        : _writer(writer)
    {
    }

    [[nodiscard]] DumpResult<void> dump(const CommitJournal& journal);

private:
    fs::FilePageWriter& _writer;
};

}
