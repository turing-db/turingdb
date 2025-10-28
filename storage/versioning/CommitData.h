#pragma once

#include "versioning/CommitHash.h"
#include "versioning/CommitHistory.h"
#include "metadata/GraphMetadata.h"
#include "versioning/Tombstones.h"

namespace db {

class DataPart;
class CommitBuilder;
class CommitLoader;
class GraphLoader;
class VersionController;
class Change;
class ChangeRebaser;

class CommitData {
public:
    explicit CommitData(CommitHash hash)
        : _hash(hash)
    {
    }

    ~CommitData() = default;

    CommitData(const CommitData&) = delete;
    CommitData(CommitData&&) = delete;
    CommitData& operator=(const CommitData&) = delete;
    CommitData& operator=(CommitData&&) = delete;

    [[nodiscard]] DataPartSpan allDataparts() const { return _history.allDataparts(); }
    [[nodiscard]] DataPartSpan commitDataparts() const { return _history.commitDataparts(); }
    [[nodiscard]] const GraphMetadata& metadata() const { return _metadata; }
    [[nodiscard]] std::span<const CommitView> commits() const { return _history.commits(); }
    [[nodiscard]] const CommitHistory& history() const { return _history; }
    [[nodiscard]] CommitHistory& history() { return _history; }
    [[nodiscard]] CommitHash hash() const { return _hash; }
    [[nodiscard]] const Tombstones& tombstones() const { return _tombstones; }

private:
    friend CommitBuilder;
    friend CommitLoader;
    friend GraphLoader;
    friend VersionController;
    friend Change;
    friend Commit;
    friend ChangeRebaser;

    CommitHash _hash;
    CommitHistory _history;
    GraphMetadata _metadata;
    Tombstones _tombstones;
};

}

