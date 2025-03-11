#pragma once

#include "DataPartSpan.h"
#include "versioning/CommitData.h"

namespace db {

class GraphReader;
class Graph;

class GraphMetadata;
class GraphView {
public:
    GraphView() = default;

    explicit GraphView(const Graph& graph, const CommitData& data)
        : _graph(&graph),
          _data(&data)
    {
    }

    bool isValid() const { return !_data; }

    [[nodiscard]] GraphReader read() const;
    [[nodiscard]] CommitHash commitHash() const { return _data->hash(); }
    [[nodiscard]] DataPartSpan dataparts() const { return _data->allDataparts(); }
    [[nodiscard]] DataPartSpan commitDataparts() const { return _data->commitDataparts(); }
    [[nodiscard]] GraphMetadata& metadata() const { return _data->metadata(); }

private:
    friend GraphReader;
    const Graph* _graph {nullptr};
    const CommitData* _data {nullptr};
};

}
