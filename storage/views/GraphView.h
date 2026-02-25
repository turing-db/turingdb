#pragma once

#include "DataPartSpan.h"
#include "versioning/CommitData.h"

namespace db {

class GraphReader;
class Graph;

class GraphView {
public:
    GraphView() = default;

    explicit GraphView(const CommitData* data)
        : _data(data)
    {
    }

    bool isValid() const { return _data; }

    [[nodiscard]] GraphReader read() const;
    [[nodiscard]] DataPartSpan dataparts() const { return _data->allDataparts(); }
    [[nodiscard]] CommitHash headCommitHash() const { return _data->hash(); }
    [[nodiscard]] DataPartSpan commitDataparts() const { return _data->commitDataparts(); }
    [[nodiscard]] const Tombstones& tombstones() const { return _data->tombstones(); }
    [[nodiscard]] const GraphMetadata& metadata() const { return _data->metadata(); }
    [[nodiscard]] const CommitHistory& history() const { return _data->history(); }

private:
    friend GraphReader;
    const CommitData* _data {nullptr};
};

}
