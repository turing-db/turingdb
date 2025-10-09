#include "CommitJournal.h"

#include "versioning/CommitWriteBuffer.h"
#include <memory>

using namespace db;

std::unique_ptr<CommitJournal> CommitJournal::newJournal(CommitWriteBuffer& wb) {
    CommitJournal* newJournal = new CommitJournal();

    newJournal->_deletedNodes = wb._deletedNodes;
    newJournal->_deletedEdges = wb._deletedEdges;

    newJournal->_perDataPartDeletedNodes = wb._perDataPartDeletedNodes;
    newJournal->_perDataPartDeletedEdges = wb._perDataPartDeletedEdges;

    newJournal->_initialised = true;

    return std::unique_ptr<CommitJournal>(newJournal);
}
