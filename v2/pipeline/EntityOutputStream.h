#pragma once

#include <stdexcept>
#include <variant>

#include "PipelineException.h"
#include "dataframe/ColumnTag.h"

namespace db::v2 {

class EntityOutputStream {
public:
    struct NodeStream {
        ColumnTag _nodeIDsTag;
    };

    struct EdgeStream {
        ColumnTag _edgeIDsTag;
        ColumnTag _otherIDsTag;
        ColumnTag _edgeTypesTag;
    };

    using StreamVariant = std::variant<std::monostate, NodeStream, EdgeStream>;

    EntityOutputStream() = default;

    static EntityOutputStream createEmptyStream() {
        return EntityOutputStream {};
    }

    static EntityOutputStream createNodeStream(ColumnTag nodeIDsTag) {
        EntityOutputStream stream;
        stream._stream = NodeStream {nodeIDsTag};
        return stream;
    }

    static EntityOutputStream createEdgeStream(ColumnTag edgeIDsTag,
                                               ColumnTag otherIDsTag,
                                               ColumnTag edgeTypesTag) {
        EntityOutputStream stream;
        stream._stream = EdgeStream {edgeIDsTag, otherIDsTag, edgeTypesTag};
        return stream;
    }

    void projectEdgeTarget() {
        if (!isEdgeStream()) {
            throw PipelineException("Cannot project edge target on non-edge stream");
        }

        _stream = NodeStream {asEdgeStream()._otherIDsTag};
    }

    bool isEmpty() const {
        return std::holds_alternative<std::monostate>(_stream);
    }

    bool isNodeStream() const {
        return std::holds_alternative<NodeStream>(_stream);
    }

    bool isEdgeStream() const {
        return std::holds_alternative<EdgeStream>(_stream);
    }

    const NodeStream& asNodeStream() const {
        return std::get<NodeStream>(_stream);
    }

    const EdgeStream& asEdgeStream() const {
        return std::get<EdgeStream>(_stream);
    }

    template <typename T>
        requires requires(T visitor) {
            { visitor(NodeStream {}) };
            { visitor(EdgeStream {}) };
        }
    struct StreamVisitor {
        T _visitor;

        using ReturnType = decltype(_visitor(std::declval<const NodeStream&>()));

        [[noreturn]] ReturnType operator()(const std::monostate& stream) const {
            throw std::runtime_error("Called StreamVisitor::operator() on empty stream");
        }

        ReturnType operator()(const NodeStream& stream) const {
            return _visitor(stream);
        }

        ReturnType operator()(const EdgeStream& stream) const {
            return _visitor(stream);
        }
    };

    auto visit(const auto& visitor) const {
        return std::visit(StreamVisitor<decltype(visitor)> {visitor}, _stream);
    }

private:
    StreamVariant _stream;
};

}
