#pragma once

#include <stddef.h>

namespace db {

class CommitHistory;
class DataPart;
template <typename T>
class WeakArc;

class Index;

class CommitHistoryBuilder {
public:
    explicit CommitHistoryBuilder(CommitHistory& history)
        : _history(history)
    {
    }

    void addDatapart(const WeakArc<DataPart>& datapart);

    void addValidIndex(const WeakArc<Index>& index);

    void setCommitDatapartCount(size_t count);

private:
    CommitHistory& _history;
};

}
