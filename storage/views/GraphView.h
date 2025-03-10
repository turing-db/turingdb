#pragma once

#include "DataPartSpan.h"
#include "versioning/CommitData.h"

namespace db {

class GraphReader;
class GraphMetadata;
class Graph;

class GraphView {
public:
    GraphView() = default;

    explicit GraphView(Graph& graph, const std::shared_ptr<const CommitData>& data)
        : _graph(&graph),
          _data(data)
    {
    }

    bool isValid() const { return !_data; }

    [[nodiscard]] const CommitData& data() const { return *_data; }
    [[nodiscard]] GraphReader read() const;
    [[nodiscard]] DataPartSpan dataparts() const { return _data->dataparts(); }
    [[nodiscard]] GraphMetadata& metadata() const { return _data->metadata(); }
    [[nodiscard]] std::unique_ptr<CommitBuilder> prepareCommit() const;

private:
    friend GraphReader;
    Graph* _graph {nullptr};

    std::shared_ptr<const CommitData> _data;
};

}
