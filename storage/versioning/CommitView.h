#pragma once

#include "DataPartSpan.h"
#include "versioning/CommitHash.h"

namespace db {

class Commit;
class CommitHistory;
class CommitData;
class VersionController;
class GraphMetadata;
class FrozenCommitTx;
class Tombstones;

class CommitView {
public:
    CommitView() = default;
    ~CommitView() = default;

    explicit CommitView(const Commit* commit)
        : _commit {commit}
    {
    }

    CommitView(const CommitView&) = default;
    CommitView(CommitView&&) = default;
    CommitView& operator=(const CommitView&) noexcept = default;
    CommitView& operator=(CommitView&&) noexcept = default;

    [[nodiscard]] bool isValid() const;
    [[nodiscard]] bool hasData() const;
    [[nodiscard]] const CommitData& data() const;
    [[nodiscard]] bool isHead() const;
    [[nodiscard]] CommitHash hash() const;
    [[nodiscard]] const VersionController& controller() const;
    [[nodiscard]] DataPartSpan dataparts() const;
    [[nodiscard]] const CommitHistory& history() const;
    [[nodiscard]] const GraphMetadata& metadata() const;
    [[nodiscard]] const Tombstones& tombstones() const;

    [[nodiscard]] FrozenCommitTx openTransaction() const;

    bool operator==(const CommitView& other) const {
        return _commit == other._commit;
    }

    bool operator!=(const CommitView& other) const {
        return !(*this == other);
    }

private:
    friend Commit;

    const Commit* _commit {nullptr};
};

}
