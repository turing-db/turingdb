#pragma once

#include "ArcManager.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitData.h"

namespace db {

class DataPart;
class CommitData;
class VersionController;
class CommitBuilder;
class CommitLoader;
class GraphLoader;
class Graph;
class Transaction;
class Change;

class Commit {
public:
    Commit();
    ~Commit();

    Commit(const Commit&) = delete;
    Commit(Commit&&) = delete;
    Commit& operator=(const Commit&) = delete;
    Commit& operator=(Commit&&) = delete;

    [[nodiscard]] bool holdsData() const {
        return _data != nullptr;
    }

    [[nodiscard]] Transaction openTransaction() const;

    [[nodiscard]] CommitHash hash() const { return _hash; }

    [[nodiscard]] const CommitData& data() const { return *_data; }
    [[nodiscard]] bool hasData() const { return _data != nullptr; }
    [[nodiscard]] const CommitHistory& history() const { return _data->history(); }
    [[nodiscard]] CommitHistory& history() { return _data->history(); }
    [[nodiscard]] bool isHead() const;

private:
    friend CommitBuilder;
    friend CommitLoader;
    friend GraphLoader;
    friend VersionController;
    friend Change;

    VersionController* _controller {nullptr};
    CommitHash _hash = CommitHash::create();
    WeakArc<CommitData> _data;
};

}
