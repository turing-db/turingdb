#pragma once

#include <variant>

namespace db::v2 {

class PipelineOutputPort;
class MaterializeData;

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

    const Stream& getStream() const { return _stream; }

    bool isNodeStream() const { return std::holds_alternative<NodeStream>(_stream); }
    bool isEdgeStream() const { return std::holds_alternative<EdgeStream>(_stream); }

    NodeStream& getNodeStream() { return std::get<NodeStream>(_stream); }
    EdgeStream& getEdgeStream() { return std::get<EdgeStream>(_stream); }

    MaterializeData* getMaterializeData() const { return _matData; }

    void setMaterializeData(MaterializeData* data) { _matData = data; }

    void closeMaterializeData() { _matData = nullptr; }

    void setStream(Stream&& stream) {
        _stream = std::move(stream);
    }

private:
    Stream _stream;
    MaterializeData* _matData {nullptr};
};

}
