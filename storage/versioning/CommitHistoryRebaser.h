#pragma once

#include <stddef.h>

#include "Commit.h"
#include "CommitWriteBuffer.h"

namespace db {

class CommitHistory;
class MetadataRebaser;
class DataPartRebaser;
class EntityIDRebaser;

class CommitHistoryRebaser {
public:
    explicit CommitHistoryRebaser(CommitHistory& history)
        : _history(history)
    {
    }

    void rebase(const MetadataRebaser& metadataRebaser,
                const EntityIDRebaser& entityRebaser,
                DataPartRebaser& dataPartRebaser,
                const CommitHistory& prevHistory);

    void removeCreatedDataParts();

    void addValidIndexes(const CommitHistory& prevHistory,
                         Commit::CommitSpan commitsSinceBranch,
                         const CommitWriteBuffer::DroppedIndexes& droppedIndexes);

private:
    CommitHistory& _history;

    void resizeDataParts(size_t newSize);

    void resetCommitDataParts();
};

}
