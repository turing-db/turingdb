#pragma once

#include "DataPartSpan.h"
#include "versioning/CommitData.h"

namespace db {

class GraphReader;
class Graph;

class GraphView {
public:
    GraphView() = default;

    explicit GraphView(const CommitData& data)
        : _data(&data)
    {
    }

    bool isValid() const { return _data; }

    [[nodiscard]] GraphReader read() const;
    [[nodiscard]] std::span<const CommitView> commits() const { return _data->commits(); }
    [[nodiscard]] DataPartSpan dataparts() const { return _data->allDataparts(); }
    [[nodiscard]] DataPartSpan commitDataparts() const { return _data->commitDataparts(); }
    [[nodiscard]] const Tombstones& tombstones() const { return _data->tombstones(); }
    [[nodiscard]] const GraphMetadata& metadata() const { return _data->metadata(); }

private:
    friend GraphReader;
    const CommitData* _data {nullptr};
};

}
