#pragma once

#include "ArcManager.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "versioning/CommitData.h"

namespace db {

class DataPart;
class CommitData;
class VersionController;
class CommitBuilder;
class CommitLoader;
class GraphLoader;
class Graph;

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

    [[nodiscard]] Transaction openTransaction() const {
        return {*_graph, _data};
    }

    [[nodiscard]] WriteTransaction openWriteTransaction() const {
        return {*_graph, _data};
    }

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

    Graph* _graph {nullptr};
    CommitHash _hash = CommitHash::create();
    WeakArc<CommitData> _data;
};

}
