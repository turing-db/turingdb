#include "CommitJournal.h"

#include <memory>

using namespace db;

std::unique_ptr<CommitJournal> CommitJournal::newJournal([[maybe_unused]] CommitWriteBuffer& wb) {
    CommitJournal* journal = new CommitJournal;
    return std::unique_ptr<CommitJournal>(journal);
}
