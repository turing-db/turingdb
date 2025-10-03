#pragma once

#include <variant>

namespace db::v2 {

class PipelineOutputPort;

class PlanGraphStream {
public:
    enum class Mode {
        UNKNOWN,
        NODE_STREAM,
        EDGE_STREAM
    };

    struct EmptyStream {
    };

    struct NodeStream {
        PipelineOutputPort* nodeIDs {nullptr};
    };

    struct EdgeStream {
        PipelineOutputPort* edgeIDs {nullptr};
        PipelineOutputPort* targetIDs {nullptr};
    };

    using Stream = std::variant<EmptyStream, NodeStream, EdgeStream>;

    explicit PlanGraphStream()
        : _stream(EmptyStream())
    {
    }

    ~PlanGraphStream() = default;

    bool isNodeStream() const { return std::holds_alternative<NodeStream>(_stream); }
    bool isEdgeStream() const { return std::holds_alternative<EdgeStream>(_stream); }

    NodeStream& getNodeStream() { return std::get<NodeStream>(_stream); }
    EdgeStream& getEdgeStream() { return std::get<EdgeStream>(_stream); }

    void set(Stream stream) {
        _stream = stream;
    }

private:
    Stream _stream;
};

}
