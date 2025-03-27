#pragma once

#include "DataPartSpan.h"
#include "versioning/CommitHash.h"

namespace db {

class Commit;

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
    [[nodiscard]] bool isHead() const;
    [[nodiscard]] CommitHash hash() const;
    [[nodiscard]] DataPartSpan dataparts() const;

    bool operator==(const CommitView& other) const {
        return _commit == other._commit;
    }

    bool operator!=(const CommitView& other) const {
        return !(*this == other);
    }

private:
    const Commit* _commit {nullptr};
};

}
