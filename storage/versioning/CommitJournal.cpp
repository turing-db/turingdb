#include "CommitJournal.h"

#include "versioning/CommitWriteBuffer.h"
#include <memory>

using namespace db;

std::unique_ptr<CommitJournal> CommitJournal::newJournal(CommitWriteBuffer& wb) {
    CommitJournal* newJournal = new CommitJournal();

    newJournal->_deletedNodes = std::move(wb._deletedNodes);
    newJournal->_deletedEdges = std::move(wb._deletedEdges);

    newJournal->_perDataPartDeletedNodes = std::move(wb._perDataPartDeletedNodes);
    newJournal->_perDataPartDeletedEdges = std::move(wb._perDataPartDeletedEdges);

    newJournal->_initialised = true;

    return std::unique_ptr<CommitJournal>(newJournal);
}
