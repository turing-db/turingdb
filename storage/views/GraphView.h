#pragma once

#include "DataPartSpan.h"

namespace js {
class JobSystem;
}

namespace db {

class GraphReader;
class GraphMetadata;

class GraphView {
public:
    GraphView() = default;

    GraphView(DataPartSpan dataparts, GraphMetadata& metadata)
        : _dataparts(dataparts),
          _metadata(&metadata)
    {
    }

    bool isValid() const {
        return !_dataparts.empty();
    }

    [[nodiscard]] GraphReader read() const;
    [[nodiscard]] DataPartSpan dataparts() const { return _dataparts; }
    [[nodiscard]] GraphMetadata& metadata() const { return *_metadata; }

private:
    friend GraphReader;

    DataPartSpan _dataparts;
    GraphMetadata* _metadata {nullptr};
};

}
