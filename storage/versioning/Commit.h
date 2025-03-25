#pragma once

#include <memory>

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

    [[nodiscard]] CommitHash hash() const { return _data->hash(); }

    [[nodiscard]] const CommitData& data() const { return *_data; }

private:
    friend CommitBuilder;
    friend CommitLoader;
    friend GraphLoader;
    friend VersionController;

    Graph* _graph {nullptr};
    WeakArc<CommitData> _data;
};

}
