#pragma once

#include <memory>

#include "ArcManager.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitData.h"

namespace db {

class DataPart;
class VersionController;
class CommitLoader;
class GraphLoader;
class FrozenCommitTx;

class Commit {
public:
    using CommitVector = std::vector<std::unique_ptr<Commit>>;
    using CommitSpan = std::span<const std::unique_ptr<Commit>>;

    Commit();
    Commit(VersionController* controller, const WeakArc<CommitData>& data);
    ~Commit();

    Commit(const Commit&) = delete;
    Commit(Commit&&) = delete;
    Commit& operator=(const Commit&) = delete;
    Commit& operator=(Commit&&) = delete;

    [[nodiscard]] bool holdsData() const {
        return _data != nullptr;
    }

    [[nodiscard]] FrozenCommitTx openTransaction() const;

    [[nodiscard]] CommitHash hash() const { return _hash; }

    [[nodiscard]] const VersionController& controller() const { return *_controller; }
    [[nodiscard]] const CommitData& data() const { return *_data; }
    [[nodiscard]] bool hasData() const { return _data != nullptr; }
    [[nodiscard]] const CommitHistory& history() const { return _data->history(); }
    [[nodiscard]] CommitHistory& history() { return _data->history(); }
    [[nodiscard]] bool isHead() const;
    [[nodiscard]] CommitView view() const;

    [[nodiscard]] static std::unique_ptr<Commit> createNextCommit(VersionController* controller,
                                                                  const WeakArc<CommitData>& data,
                                                                  const CommitView& prevCommit);

private:
    friend CommitLoader;
    friend GraphLoader;

    VersionController* _controller {nullptr};
    CommitHash _hash = CommitHash::create();
    WeakArc<CommitData> _data;
};

}
