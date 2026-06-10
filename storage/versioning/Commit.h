#pragma once

#include <memory>

#include "ArcManager.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitData.h"

namespace db {

class DataPart;
class VersionController;
class CommitLoader;
class CommitParquetLoader;
class GraphLoader;
class FrozenCommitTx;
class GraphView;

class Commit {
public:
    using CommitVector = std::vector<std::unique_ptr<Commit>>;
    using CommitSpan = std::span<const std::unique_ptr<Commit>>;

    Commit();
    Commit(VersionController* controller,
           const WeakArc<CommitData>& data,
           const Commit* prevCommit);
    Commit(VersionController* controller, CommitHash hash, const Commit* prevCommit);
    ~Commit();

    Commit(const Commit&) = delete;
    Commit(Commit&&) = delete;
    Commit& operator=(const Commit&) = delete;
    Commit& operator=(Commit&&) = delete;

    [[nodiscard]] FrozenCommitTx openTransaction() const;

    [[nodiscard]] CommitHash hash() const { return _hash; }

    [[nodiscard]] const VersionController& controller() const { return *_controller; }
    [[nodiscard]] const CommitData& data() const { return *_data; }
    [[nodiscard]] bool hasData() const { return _data != nullptr; }
    [[nodiscard]] const CommitHistory& history() const { return _data->history(); }
    [[nodiscard]] CommitHistory& history() { return _data->history(); }
    [[nodiscard]] bool isHead() const;
    [[nodiscard]] bool isMergeCommit() const { return _mergeCommit; };
    [[nodiscard]] CommitView view() const;

    [[nodiscard]] static std::unique_ptr<Commit> createNextCommit(VersionController* controller,
                                                                  const WeakArc<CommitData>& data,
                                                                  const Commit* commit);

    [[nodiscard]] static std::unique_ptr<Commit> createMergeCommit(VersionController* controller,
                                                                   const WeakArc<CommitData>& data,
                                                                   const Commit* commit);

    size_t getNumNodes() const { return _numNodes; }
    size_t getNumEdges() const { return _numEdges; }
    size_t getNumDataParts() const { return _numDataParts; }

    const Commit* getPreviousCommit() const { return _prevCommit; }

private:
    friend CommitLoader;
    friend CommitParquetLoader;
    friend GraphLoader;
    friend CommitBuilder;
    friend Change;

    VersionController* _controller {nullptr};
    CommitHash _hash;
    WeakArc<CommitData> _data;
    const Commit* _prevCommit {nullptr};

    bool _mergeCommit {false};
    size_t _numNodes {0};
    size_t _numEdges {0};
    size_t _numDataParts {0};

    void setCommitData(const WeakArc<CommitData>& data) { _data = data;};
};
}
