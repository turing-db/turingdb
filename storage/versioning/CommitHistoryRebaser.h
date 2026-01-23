#pragma once

#include <stddef.h>

namespace db {

class CommitHistory;
class MetadataRebaser;
class DataPartRebaser;

class CommitHistoryRebaser {
public:
    explicit CommitHistoryRebaser(CommitHistory& history)
        : _history(history)
    {
    }

    void rebase(const MetadataRebaser& metadataRebaser,
                DataPartRebaser& dataPartRebaser,
                const CommitHistory& prevHistory);

    void removeCreatedDataParts();

private:
    CommitHistory& _history;

    void resizeDataParts(size_t newSize);

    void resetCommitDataParts();
};

}
