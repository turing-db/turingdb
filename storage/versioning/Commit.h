#pragma once

#include <memory>

#include "versioning/CommitHash.h"

namespace db {

class DataPart;
class CommitData;
class VersionController;
class CommitBuilder;
class GraphView;
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

    [[nodiscard]] GraphView view() const;
    [[nodiscard]] CommitHash hash() const { return _hash; }

private:
    friend CommitBuilder;
    friend VersionController;

    Graph* _graph {nullptr};
    CommitHash _hash = CommitHash::create();
    std::shared_ptr<CommitData> _data;
};

}
